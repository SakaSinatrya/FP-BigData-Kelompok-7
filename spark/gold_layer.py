"""
Gold Layer — SembakoWatch
Agregasi bulanan kurs, harga pangan, dan harga minyak (semua dari Silver),
tempel inflasi BPS sebagai pembanding, hitung korelasi Pearson per komoditas
(vs kurs & vs minyak), lalu ekspor JSON ke dashboard/data/gold/ untuk Flask.

Fitur ML:
  1. Forecasting  — PySpark ML Linear Regression per komoditas
  2. Anomaly Detection — Z-Score residual untuk deteksi penimbunan/gagal panen
  3. HET Early Warning — Peringatan dini Harga Eceran Tertinggi
"""

import os
import json
os.environ["HADOOP_USER_NAME"] = "root"

import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, avg, round as spark_round,
    corr as spark_corr, lit, monotonically_increasing_id
)
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

HDFS_SILVER_PATH    = "hdfs://localhost:8020/data/lakehouse/silver"
HDFS_BRONZE_BPS     = "hdfs://localhost:8020/data/lakehouse/bronze_parquet/bps"
HDFS_GOLD_PATH      = "hdfs://localhost:8020/data/lakehouse/gold"
LOCAL_GOLD_DIR      = "dashboard/data/gold"

WEEKS_AHEAD = 4   # horizon proyeksi dampak kurs ke harga

# ── Harga Eceran Tertinggi (HET) — referensi kebijakan pemerintah ───────────
HET_THRESHOLDS = {
    "Beras Medium":       15500,
    "Bawang Putih Impor": 60000,
    "Cabai Rawit Lokal":  80000,
}

# ─────────────────────────────────────────────────────────────────────────────
#  Utility Functions (existing)
# ─────────────────────────────────────────────────────────────────────────────

def import_weight(nama_komoditas: str) -> float:
    n = nama_komoditas.lower()
    if "impor" in n:
        return 1.0
    if "lokal" in n:
        return 0.2
    return 0.5  # status tidak diketahui / semi-impor


def generate_insight(kom: str, corr_kurs: float, corr_oil, import_w: float,
                      fpvi_level: str, status: str) -> str:
    parts = []

    if import_w >= 1.0:
        parts.append(
            f"{kom} adalah komoditas IMPOR - harga dibeli dalam USD, sehingga "
            f"depresiasi rupiah menaikkan biaya pokok secara LANGSUNG "
            f"(korelasi r={corr_kurs:.2f} konsisten dengan kanal kausal ini)."
        )
    elif import_w <= 0.2:
        parts.append(
            f"{kom} adalah komoditas LOKAL - secara teori TIDAK terdampak kurs "
            f"secara langsung. Korelasi yang teramati (r={corr_kurs:.2f}) lebih "
            f"mungkin disebabkan faktor lain: musim tanam/panen, cuaca, biaya "
            f"distribusi, atau kebetulan tren waktu yang sejalan dengan kurs - "
            f"BUKAN hubungan kausal langsung dari kurs ke harga."
        )
    else:
        parts.append(
            f"{kom} berstatus ketergantungan impor tidak pasti (semi-impor) - "
            f"pengaruh kurs (r={corr_kurs:.2f}) kemungkinan datang dari komponen "
            f"input produksi (pupuk, pakan, BBM) yang sebagian diimpor, bukan "
            f"dari komoditas itu sendiri."
        )

    if corr_oil is not None and abs(corr_oil) >= 0.3:
        arah = "naik" if corr_oil > 0 else "turun"
        parts.append(
            f"Korelasi dengan harga minyak Brent (r={corr_oil:.2f}) menunjukkan "
            f"biaya distribusi/transportasi turut berperan - saat harga minyak "
            f"{arah}, harga {kom} cenderung mengikuti arah yang sama."
        )

    if status == "RAWAN NAIK":
        parts.append("Tren kurs bulan terakhir menguat - risiko harga naik dalam waktu dekat.")
    elif status == "POTENSI TURUN":
        parts.append("Tren kurs melemah - ada potensi harga turun/stabil dalam waktu dekat.")

    prioritas = {"Tinggi": "prioritas pemantauan tinggi", "Sedang": "pemantauan rutin", "Rendah": "risiko rendah saat ini"}
    parts.append(f"Skor FPVI: {fpvi_level} ({prioritas.get(fpvi_level, '-')}).")

    return " ".join(parts)


# ─────────────────────────────────────────────────────────────────────────────
#  ML Feature 1: Forecasting (PySpark ML Linear Regression)
# ─────────────────────────────────────────────────────────────────────────────

def run_ml_forecasting(spark, monthly_joined, kurs_data, pangan_by_kom):
    """
    Melatih model Linear Regression per komoditas menggunakan PySpark ML.
    Feature: avg_kurs  |  Label: avg_harga
    Output: prediksi harga 1-4 minggu ke depan berdasarkan proyeksi kurs.
    """
    print("\n[ML] ═══ Memulai ML Forecasting (PySpark ML Linear Regression) ═══")

    kurs_sorted = sorted(
        [d for d in kurs_data if d["avg_kurs"] is not None],
        key=lambda d: (d["year"], d["month"])
    )
    if len(kurs_sorted) < 3:
        print("[ML] WARN: Data kurs < 3 bulan, forecasting dilewati")
        return {"model_info": {"algorithm": "PySpark ML Linear Regression", "status": "SKIPPED", "reason": "Data kurang dari 3 bulan"}, "forecasts": []}

    kurs_terakhir = kurs_sorted[-1]["avg_kurs"]
    kurs_trend = kurs_sorted[-1]["avg_kurs"] - kurs_sorted[-2]["avg_kurs"]
    weekly_trend = kurs_trend / 4.345  # trend per minggu

    komoditas_list = list(pangan_by_kom.keys())
    forecasts = []

    for kom in komoditas_list:
        try:
            # Filter data untuk komoditas ini
            kom_df = monthly_joined.filter(col("komoditas") == kom) \
                .select(
                    col("avg_kurs").cast("double"),
                    col("avg_harga").cast("double")
                ).dropna()

            row_count = kom_df.count()
            if row_count < 3:
                print(f"[ML]   {kom}: SKIP (data hanya {row_count} bulan)")
                continue

            # Assemble fitur untuk ML
            assembler = VectorAssembler(
                inputCols=["avg_kurs"],
                outputCol="features"
            )
            assembled_df = assembler.transform(kom_df)

            # Train Linear Regression model
            lr = LinearRegression(
                featuresCol="features",
                labelCol="avg_harga",
                maxIter=100,
                regParam=0.01,
                elasticNetParam=0.0
            )
            model = lr.fit(assembled_df)

            # Ekstrak koefisien model untuk prediksi langsung (tanpa serialisasi)
            coeff = float(model.coefficients[0])
            intcpt = float(model.intercept)

            # Evaluate model pada data training
            predictions = model.transform(assembled_df)
            evaluator_rmse = RegressionEvaluator(
                labelCol="avg_harga", predictionCol="prediction", metricName="rmse"
            )
            evaluator_r2 = RegressionEvaluator(
                labelCol="avg_harga", predictionCol="prediction", metricName="r2"
            )
            rmse = evaluator_rmse.evaluate(predictions)
            r2 = evaluator_r2.evaluate(predictions)

            # Ambil harga terakhir
            series = pangan_by_kom.get(kom, [])
            sorted_series = sorted(series, key=lambda d: (d["year"], d["month"]))
            current_price = sorted_series[-1]["avg_harga"] if sorted_series else 0

            # Prediksi 4 minggu ke depan menggunakan koefisien langsung
            # Formula: predicted_price = coefficient * projected_kurs + intercept
            # (identik secara matematis dengan model.transform())
            week_predictions = []
            for week in range(1, WEEKS_AHEAD + 1):
                projected_kurs = kurs_terakhir + weekly_trend * week
                predicted_price = max(0, coeff * projected_kurs + intcpt)

                week_predictions.append({
                    "week": week,
                    "predicted_price": round(predicted_price),
                    "kurs_assumed": round(projected_kurs, 2)
                })

            forecasts.append({
                "komoditas": kom,
                "rmse": round(rmse, 2),
                "r2_score": round(r2, 4),
                "current_price": round(current_price),
                "coefficient": round(coeff, 4),
                "intercept": round(intcpt, 2),
                "training_samples": row_count,
                "predictions": week_predictions
            })

            trend_emoji = "📈" if week_predictions[-1]["predicted_price"] > current_price else "📉"
            print(f"[ML]   {kom}: R²={r2:.4f}, RMSE={rmse:.0f} {trend_emoji} "
                  f"Prediksi Minggu-4: Rp {week_predictions[-1]['predicted_price']:,.0f}")

        except Exception as e:
            print(f"[ML]   {kom}: ERROR - {e}")
            continue

    result = {
        "model_info": {
            "algorithm": "PySpark ML Linear Regression",
            "features": ["avg_kurs"],
            "training_months": len(kurs_sorted),
            "status": "OK"
        },
        "forecasts": forecasts
    }

    print(f"[ML] ═══ Forecasting selesai: {len(forecasts)} model terlatih ═══\n")
    return result


# ─────────────────────────────────────────────────────────────────────────────
#  ML Feature 2: Anomaly Detection (Z-Score Residual)
# ─────────────────────────────────────────────────────────────────────────────

def run_anomaly_detection(kurs_data, pangan_by_kom, corr_data):
    """
    Deteksi anomali harga: jika harga aktual menyimpang jauh dari
    harga yang diperkirakan berdasarkan model regresi (kurs → harga),
    tandai sebagai anomali — kemungkinan penimbunan, gagal panen,
    atau gangguan distribusi.
    """
    print("[ML] ═══ Memulai Anomaly Detection (Z-Score Residual) ═══")

    kurs_sorted = sorted(
        [d for d in kurs_data if d["avg_kurs"] is not None],
        key=lambda d: (d["year"], d["month"])
    )
    if len(kurs_sorted) < 3:
        print("[ML] WARN: Data kurs < 3 bulan, anomaly detection dilewati")
        return {"anomalies": [], "summary": {"total_anomalies": 0, "high_severity": 0, "medium_severity": 0, "low_severity": 0}}

    kurs_lookup = {(d["year"], d["month"]): d["avg_kurs"] for d in kurs_sorted}

    all_anomalies = []

    for kom, series in pangan_by_kom.items():
        pairs = [
            (kurs_lookup[(d["year"], d["month"])], d["avg_harga"], d["year"], d["month"])
            for d in series if (d["year"], d["month"]) in kurs_lookup
        ]
        if len(pairs) < 3:
            continue

        kurs_vals = np.array([p[0] for p in pairs], dtype=float)
        harga_vals = np.array([p[1] for p in pairs], dtype=float)

        # Fit regresi linear
        try:
            slope, intercept = np.polyfit(kurs_vals, harga_vals, 1)
        except Exception:
            continue

        # Hitung residual (selisih harga aktual vs harga expected)
        expected = slope * kurs_vals + intercept
        residuals = harga_vals - expected
        mean_res = np.mean(residuals)
        std_res = np.std(residuals)

        if std_res < 1:  # guard against near-zero std
            continue

        z_scores = (residuals - mean_res) / std_res

        for i, (kurs, harga, yr, mo) in enumerate(pairs):
            z = abs(z_scores[i])
            if z >= 1.8:  # threshold anomali
                deviasi_persen = round(100 * (harga - expected[i]) / expected[i], 1)

                if z >= 3.0:
                    severity = "HIGH"
                    penyebab = "Lonjakan harga SANGAT tidak wajar — kemungkinan besar penimbunan massal atau krisis pasokan serius"
                elif z >= 2.5:
                    severity = "HIGH"
                    penyebab = "Lonjakan harga tidak wajar — kemungkinan penimbunan atau gagal panen"
                elif z >= 2.0:
                    severity = "MEDIUM"
                    penyebab = "Harga menyimpang signifikan — kemungkinan gangguan distribusi atau spekulasi pasar"
                else:
                    severity = "LOW"
                    penyebab = "Harga sedikit tidak wajar — perlu pemantauan lebih lanjut"

                all_anomalies.append({
                    "komoditas": kom,
                    "bulan": f"{yr:04d}-{mo:02d}",
                    "harga_aktual": round(harga),
                    "harga_expected": round(expected[i]),
                    "deviasi_persen": deviasi_persen,
                    "z_score": round(z_scores[i], 2),
                    "severity": severity,
                    "kemungkinan_penyebab": penyebab
                })

    # Sort anomalies by severity then by z_score
    severity_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    all_anomalies.sort(key=lambda a: (severity_order.get(a["severity"], 3), -abs(a["z_score"])))

    high = sum(1 for a in all_anomalies if a["severity"] == "HIGH")
    medium = sum(1 for a in all_anomalies if a["severity"] == "MEDIUM")
    low = sum(1 for a in all_anomalies if a["severity"] == "LOW")

    result = {
        "anomalies": all_anomalies,
        "summary": {
            "total_anomalies": len(all_anomalies),
            "high_severity": high,
            "medium_severity": medium,
            "low_severity": low
        }
    }

    print(f"[ML]   Terdeteksi {len(all_anomalies)} anomali "
          f"(HIGH: {high}, MEDIUM: {medium}, LOW: {low})")
    print("[ML] ═══ Anomaly Detection selesai ═══\n")
    return result


# ─────────────────────────────────────────────────────────────────────────────
#  ML Feature 3: HET Early Warning System
# ─────────────────────────────────────────────────────────────────────────────

def run_het_warning(pangan_by_kom, forecast_data):
    """
    Bandingkan harga terakhir dan harga proyeksi ML terhadap
    Harga Eceran Tertinggi (HET) yang ditetapkan pemerintah.
    """
    print("[ML] ═══ Memulai HET Early Warning System ═══")

    warnings = []
    forecast_lookup = {}
    for fc in forecast_data.get("forecasts", []):
        forecast_lookup[fc["komoditas"]] = fc

    for kom, het_value in HET_THRESHOLDS.items():
        series = pangan_by_kom.get(kom, [])
        if not series:
            continue

        sorted_series = sorted(series, key=lambda d: (d["year"], d["month"]))
        harga_terakhir = sorted_series[-1]["avg_harga"]
        persen_dari_het = round(100 * harga_terakhir / het_value, 1)

        # Tentukan status
        if persen_dari_het >= 120:
            status = "DARURAT"
        elif persen_dari_het >= 100:
            status = "KRITIS"
        elif persen_dari_het >= 80:
            status = "WASPADA"
        else:
            status = "AMAN"

        # Cek proyeksi ML
        proyeksi_tembus = False
        estimasi_minggu_tembus = None
        fc = forecast_lookup.get(kom)
        if fc and fc.get("predictions"):
            for pred in fc["predictions"]:
                if pred["predicted_price"] >= het_value:
                    proyeksi_tembus = True
                    estimasi_minggu_tembus = pred["week"]
                    break

        # Hitung tren harga dari 2 bulan terakhir
        tren_harga = None
        if len(sorted_series) >= 2:
            tren_harga = round(sorted_series[-1]["avg_harga"] - sorted_series[-2]["avg_harga"])

        warnings.append({
            "komoditas": kom,
            "harga_terakhir": round(harga_terakhir),
            "het": het_value,
            "persen_dari_het": persen_dari_het,
            "status": status,
            "proyeksi_tembus_het": proyeksi_tembus,
            "estimasi_minggu_tembus": estimasi_minggu_tembus,
            "tren_harga_bulanan": tren_harga
        })

        emoji = {"AMAN": "🟢", "WASPADA": "🟡", "KRITIS": "🔴", "DARURAT": "🚨"}
        print(f"[ML]   {emoji.get(status, '⚪')} {kom}: "
              f"Rp {harga_terakhir:,.0f} / HET Rp {het_value:,.0f} "
              f"({persen_dari_het:.1f}%) → {status}"
              f"{' ⚠️ PROYEKSI TEMBUS!' if proyeksi_tembus else ''}")

    result = {
        "het_thresholds": HET_THRESHOLDS,
        "warnings": warnings
    }

    print("[ML] ═══ HET Early Warning selesai ═══\n")
    return result


# ─────────────────────────────────────────────────────────────────────────────
#  Existing: FPVI Analytics (preserved)
# ─────────────────────────────────────────────────────────────────────────────

def compute_analytics(kurs_data: list, pangan_by_kom: dict, corr_data: list) -> dict:
    kurs_sorted = sorted(
        [d for d in kurs_data if d["avg_kurs"] is not None],
        key=lambda d: (d["year"], d["month"])
    )
    if len(kurs_sorted) < 2:
        return {"error": "Data kurs bulanan tidak cukup untuk analitik (butuh >= 2 bulan)"}

    kurs_lookup = {(d["year"], d["month"]): d["avg_kurs"] for d in kurs_sorted}
    kurs_trend_per_bulan = kurs_sorted[-1]["avg_kurs"] - kurs_sorted[-2]["avg_kurs"]
    months_ahead = WEEKS_AHEAD / 4.345
    kurs_terakhir = kurs_sorted[-1]["avg_kurs"]
    kurs_proyeksi = kurs_terakhir + kurs_trend_per_bulan * months_ahead

    per_komoditas = []
    for r in corr_data:
        kom = r["komoditas"]
        series = pangan_by_kom.get(kom, [])
        pairs = [
            (kurs_lookup[(d["year"], d["month"])], d["avg_harga"])
            for d in series if (d["year"], d["month"]) in kurs_lookup
        ]
        if len(pairs) < 2:
            continue

        kurs_vals  = np.array([p[0] for p in pairs], dtype=float)
        harga_vals = np.array([p[1] for p in pairs], dtype=float)

        try:
            slope, intercept = np.polyfit(kurs_vals, harga_vals, 1)
        except Exception:
            slope, intercept = 0.0, float(harga_vals.mean())

        per_komoditas.append({
            "komoditas": kom,
            "slope": float(slope),
            "intercept": float(intercept),
            "volatility": float(np.std(harga_vals)),
            "corr_kurs": r["pearson_corr"],
            "corr_strength": abs(r["pearson_corr"]),
            "corr_oil": r.get("pearson_corr_oil"),
            "latest_harga": float(harga_vals[-1]),
        })

    if not per_komoditas:
        return {"error": "Tidak ada komoditas dengan data harga+kurs yang cukup untuk analitik"}

    vols = [a["volatility"] for a in per_komoditas]
    vmin, vmax = min(vols), max(vols)

    def norm_vol(v):
        return 0.0 if vmax == vmin else (v - vmin) / (vmax - vmin)

    results = []
    for a in per_komoditas:
        fpvi = round(100 * (
            0.5 * a["corr_strength"] +
            0.3 * norm_vol(a["volatility"]) +
            0.2 * import_weight(a["komoditas"])
        ), 1)
        level = "Tinggi" if fpvi >= 65 else "Sedang" if fpvi >= 35 else "Rendah"

        if a["corr_strength"] >= 0.5 and kurs_trend_per_bulan > 0:
            status = "RAWAN NAIK"
        elif a["corr_strength"] >= 0.5 and kurs_trend_per_bulan < 0:
            status = "POTENSI TURUN"
        else:
            status = "STABIL"

        proyeksi_harga = a["slope"] * kurs_proyeksi + a["intercept"]
        dampak_persen = (
            round(100 * (proyeksi_harga - a["latest_harga"]) / a["latest_harga"], 2)
            if a["latest_harga"] else None
        )

        iw = import_weight(a["komoditas"])
        insight = generate_insight(
            a["komoditas"], a["corr_kurs"], a["corr_oil"], iw, level, status
        )

        results.append({
            "komoditas": a["komoditas"],
            "fpvi_score": fpvi,
            "fpvi_level": level,
            "status_peringatan": status,
            "harga_terakhir": round(a["latest_harga"], 0),
            "proyeksi_harga": round(proyeksi_harga, 0),
            "estimasi_dampak_persen": dampak_persen,
            "insight": insight,
        })

    results.sort(key=lambda r: -r["fpvi_score"])

    return {
        "horizon_minggu": WEEKS_AHEAD,
        "kurs_terakhir": round(kurs_terakhir, 2),
        "kurs_trend_per_bulan": round(kurs_trend_per_bulan, 2),
        "kurs_proyeksi": round(kurs_proyeksi, 2),
        "komoditas": results,
    }


# ─────────────────────────────────────────────────────────────────────────────
#  Main: Gold Layer Pipeline
# ─────────────────────────────────────────────────────────────────────────────

def create_gold_layer():
    spark = SparkSession.builder \
        .appName("GoldLayer-SembakoWatch") \
        .master("local[*]") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # ── Baca Silver Layer ────────────────────────────────────────────────────
    df = spark.read.parquet(f"{HDFS_SILVER_PATH}/kurs_pangan")

    df = df \
        .withColumn("year",  year(col("date"))) \
        .withColumn("month", month(col("date")))

    # ── Agregasi Bulanan ─────────────────────────────────────────────────────
    monthly_kurs = df \
        .groupBy("year", "month") \
        .agg(
            spark_round(avg("rate"), 2).alias("avg_kurs"),
            spark_round(avg("oil_price_usd"), 2).alias("avg_oil_usd")
        ) \
        .orderBy("year", "month")

    monthly_pangan = df \
        .groupBy("year", "month", "komoditas") \
        .agg(spark_round(avg("harga"), 0).alias("avg_harga")) \
        .orderBy("year", "month", "komoditas")

    # ── Join inflasi BPS (opsional) ──────────────────────────────────────────
    try:
        bps_df = spark.read.parquet(HDFS_BRONZE_BPS) \
            .select(
                col("year").cast("int").alias("year"),
                col("month").cast("int").alias("month"),
                col("inflasi_mtm_persen").cast("double").alias("inflasi_bps_persen")
            )
        monthly_kurs = monthly_kurs.join(bps_df, on=["year", "month"], how="left")
        has_bps = True
    except Exception as e:
        print(f"[WARN] BPS bronze tidak tersedia, skip join inflasi: {e}")
        has_bps = False

    # ── Join kurs + pangan ───────────────────────────────────────────────────
    monthly_joined = monthly_kurs.join(monthly_pangan, on=["year", "month"], how="inner")

    # ── Korelasi Pearson ─────────────────────────────────────────────────────
    correlation_df = monthly_joined \
        .groupBy("komoditas") \
        .agg(
            spark_round(spark_corr("avg_kurs", "avg_harga"), 4).alias("pearson_corr_kurs"),
            spark_round(spark_corr("avg_oil_usd", "avg_harga"), 4).alias("pearson_corr_oil")
        ) \
        .orderBy("komoditas")

    # ── Simpan Parquet ke HDFS ───────────────────────────────────────────────
    monthly_kurs.write.mode("overwrite").parquet(f"{HDFS_GOLD_PATH}/monthly_kurs")
    monthly_pangan.write.mode("overwrite").parquet(f"{HDFS_GOLD_PATH}/monthly_pangan")
    correlation_df.write.mode("overwrite").parquet(f"{HDFS_GOLD_PATH}/correlation")

    print("Gold Parquet tersimpan di HDFS (kurs+oil+inflasi BPS, pangan, korelasi)")

    # ── Ekspor JSON ke lokal ─────────────────────────────────────────────────
    os.makedirs(LOCAL_GOLD_DIR, exist_ok=True)

    kurs_rows = monthly_kurs.orderBy("year", "month").collect()
    kurs_data = [
        {
            "year": r["year"],
            "month": r["month"],
            "avg_kurs": float(r["avg_kurs"]) if r["avg_kurs"] is not None else None,
            "avg_oil_usd": float(r["avg_oil_usd"]) if r["avg_oil_usd"] is not None else None,
            "inflasi_bps_persen": float(r["inflasi_bps_persen"]) if has_bps and r["inflasi_bps_persen"] is not None else None,
        }
        for r in kurs_rows
    ]
    with open(f"{LOCAL_GOLD_DIR}/monthly_kurs.json", "w", encoding="utf-8") as f:
        json.dump(kurs_data, f, indent=2)

    pangan_rows = monthly_pangan.collect()
    pangan_by_kom = {}
    for r in pangan_rows:
        k = r["komoditas"]
        pangan_by_kom.setdefault(k, []).append(
            {"year": r["year"], "month": r["month"], "avg_harga": float(r["avg_harga"])}
        )
    with open(f"{LOCAL_GOLD_DIR}/monthly_pangan.json", "w", encoding="utf-8") as f:
        json.dump(pangan_by_kom, f, indent=2, ensure_ascii=False)

    corr_rows = correlation_df.collect()
    corr_data = [
        {
            "komoditas": r["komoditas"],
            "pearson_corr": float(r["pearson_corr_kurs"]) if r["pearson_corr_kurs"] is not None else 0.0,
            "pearson_corr_oil": float(r["pearson_corr_oil"]) if r["pearson_corr_oil"] is not None else None,
        }
        for r in corr_rows
    ]
    with open(f"{LOCAL_GOLD_DIR}/correlation.json", "w", encoding="utf-8") as f:
        json.dump(corr_data, f, indent=2, ensure_ascii=False)

    print(f"Gold JSON diekspor ke {LOCAL_GOLD_DIR}/")
    print(f"   - monthly_kurs.json   ({len(kurs_data)} baris)")
    print(f"   - monthly_pangan.json ({len(pangan_rows)} baris)")
    print(f"   - correlation.json    ({len(corr_data)} komoditas)")

    # ── Existing: FPVI Analytics ─────────────────────────────────────────────
    analytics = compute_analytics(kurs_data, pangan_by_kom, corr_data)
    with open(f"{LOCAL_GOLD_DIR}/analytics.json", "w", encoding="utf-8") as f:
        json.dump(analytics, f, indent=2, ensure_ascii=False)

    if "error" in analytics:
        print(f"[WARN] Analytics: {analytics['error']}")
    else:
        print(f"   - analytics.json      ({len(analytics['komoditas'])} komoditas)")

    # ══════════════════════════════════════════════════════════════════════════
    #  NEW: Machine Learning Features
    # ══════════════════════════════════════════════════════════════════════════

    # ── ML 1: Forecasting ────────────────────────────────────────────────────
    try:
        forecast_data = run_ml_forecasting(spark, monthly_joined, kurs_data, pangan_by_kom)
        with open(f"{LOCAL_GOLD_DIR}/forecast.json", "w", encoding="utf-8") as f:
            json.dump(forecast_data, f, indent=2, ensure_ascii=False)
        print(f"   - forecast.json       ({len(forecast_data['forecasts'])} model)")
    except Exception as e:
        print(f"[ERROR] ML Forecasting gagal: {e}")
        forecast_data = {"model_info": {"status": "ERROR"}, "forecasts": []}

    # ── ML 2: Anomaly Detection ──────────────────────────────────────────────
    try:
        anomaly_data = run_anomaly_detection(kurs_data, pangan_by_kom, corr_data)
        with open(f"{LOCAL_GOLD_DIR}/anomaly.json", "w", encoding="utf-8") as f:
            json.dump(anomaly_data, f, indent=2, ensure_ascii=False)
        print(f"   - anomaly.json        ({anomaly_data['summary']['total_anomalies']} anomali)")
    except Exception as e:
        print(f"[ERROR] Anomaly Detection gagal: {e}")
        anomaly_data = {"anomalies": [], "summary": {"total_anomalies": 0}}

    # ── ML 3: HET Early Warning ──────────────────────────────────────────────
    try:
        het_data = run_het_warning(pangan_by_kom, forecast_data)
        with open(f"{LOCAL_GOLD_DIR}/het_warning.json", "w", encoding="utf-8") as f:
            json.dump(het_data, f, indent=2, ensure_ascii=False)
        print(f"   - het_warning.json    ({len(het_data['warnings'])} komoditas)")
    except Exception as e:
        print(f"[ERROR] HET Warning gagal: {e}")

    print("\n✅ Gold Layer + ML Pipeline selesai!")
    spark.stop()


if __name__ == "__main__":
    create_gold_layer()