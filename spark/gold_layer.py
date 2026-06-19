"""
Gold Layer
Agregasi bulanan kurs, harga pangan, dan harga minyak (semua dari Silver),
tempel inflasi BPS sebagai pembanding, hitung korelasi Pearson per komoditas
(vs kurs & vs minyak), lalu ekspor JSON ke dashboard/data/gold/ untuk Flask.
"""

import os
import json
os.environ["HADOOP_USER_NAME"] = "root"

import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, avg, round as spark_round, corr as spark_corr

HDFS_SILVER_PATH    = "hdfs://localhost:8020/data/lakehouse/silver"
HDFS_BRONZE_BPS     = "hdfs://localhost:8020/data/lakehouse/bronze_parquet/bps"
HDFS_GOLD_PATH      = "hdfs://localhost:8020/data/lakehouse/gold"
LOCAL_GOLD_DIR      = "dashboard/data/gold"

WEEKS_AHEAD = 4   # horizon proyeksi dampak kurs ke harga

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


def create_gold_layer():
    spark = SparkSession.builder \
        .appName("GoldLayer") \
        .master("local[*]") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(f"{HDFS_SILVER_PATH}/kurs_pangan")

    df = df \
        .withColumn("year",  year(col("date"))) \
        .withColumn("month", month(col("date")))

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

    monthly_joined = monthly_kurs.join(monthly_pangan, on=["year", "month"], how="inner")

    correlation_df = monthly_joined \
        .groupBy("komoditas") \
        .agg(
            spark_round(spark_corr("avg_kurs", "avg_harga"), 4).alias("pearson_corr_kurs"),
            spark_round(spark_corr("avg_oil_usd", "avg_harga"), 4).alias("pearson_corr_oil")
        ) \
        .orderBy("komoditas")

    monthly_kurs.write.mode("overwrite").parquet(f"{HDFS_GOLD_PATH}/monthly_kurs")
    monthly_pangan.write.mode("overwrite").parquet(f"{HDFS_GOLD_PATH}/monthly_pangan")
    correlation_df.write.mode("overwrite").parquet(f"{HDFS_GOLD_PATH}/correlation")

    print("Gold Parquet tersimpan di HDFS (kurs+oil+inflasi BPS, pangan, korelasi)")

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

    analytics = compute_analytics(kurs_data, pangan_by_kom, corr_data)
    with open(f"{LOCAL_GOLD_DIR}/analytics.json", "w", encoding="utf-8") as f:
        json.dump(analytics, f, indent=2, ensure_ascii=False)

    if "error" in analytics:
        print(f"[WARN] Analytics: {analytics['error']}")
    else:
        print(f"   - analytics.json      ({len(analytics['komoditas'])} komoditas)")

    spark.stop()


if __name__ == "__main__":
    create_gold_layer()