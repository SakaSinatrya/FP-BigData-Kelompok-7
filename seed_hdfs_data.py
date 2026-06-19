"""
Seed HDFS dengan data historis kurs+pangan (format sama dengan yang
diharapkan bronze_layer.py milik tim).
"""

import json
import os
import random
import math
from datetime import date, timedelta
from urllib.parse import urlparse, urlunparse
import requests
from requests.adapters import HTTPAdapter

HDFS_WEB_URL  = "http://localhost:9870"
HDFS_USER     = "root"
BRONZE_KURS   = "/data/lakehouse/bronze/kurs/"
BRONZE_PANGAN = "/data/lakehouse/bronze/pangan/"


class DockerHostRewrite(HTTPAdapter):
    def send(self, request, **kwargs):
        parsed = urlparse(request.url)
        if parsed.hostname not in ("localhost", "127.0.0.1", None):
            port = parsed.port
            netloc = f"localhost:{port}" if port else "localhost"
            request.url = urlunparse(parsed._replace(netloc=netloc))
        return super().send(request, **kwargs)


def make_hdfs_client():
    from hdfs import InsecureClient
    session = requests.Session()
    session.mount("http://", DockerHostRewrite())
    return InsecureClient(HDFS_WEB_URL, user=HDFS_USER, session=session)


def gen_kurs_data(start: date, end: date) -> list[dict]:
    records = []
    rate = 15800.0
    drift = 0.3
    volatility = 60.0

    current = start
    while current <= end:
        if current.weekday() < 5:
            noise = random.gauss(drift, volatility)
            rate = max(14000, min(20000, rate + noise))
            spread = rate * 0.005
            records.append({
                "timestamp": current.isoformat(),
                "rate": round(rate, 2),
                "beli": round(rate - spread / 2, 2),
                "jual": round(rate + spread / 2, 2),
            })
        current += timedelta(days=1)
    return records


def gen_pangan_data(kurs_records: list[dict]) -> list[dict]:
    KOMODITAS = {
        "Bawang Putih Impor": {"base": 45000, "corr": 0.8,  "vol": 800},
        "Beras Medium":       {"base": 14000, "corr": 0.35, "vol": 150},
        "Cabai Rawit Lokal":  {"base": 50000, "corr": 0.05, "vol": 3000},
    }

    rates = [r["rate"] for r in kurs_records]
    mean_r = sum(rates) / len(rates)
    std_r  = math.sqrt(sum((x - mean_r) ** 2 for x in rates) / len(rates)) or 1

    records = []
    for r in kurs_records:
        z_kurs = (r["rate"] - mean_r) / std_r
        for nama, cfg in KOMODITAS.items():
            corr_effect = z_kurs * cfg["corr"] * cfg["vol"] * 2
            noise       = random.gauss(0, cfg["vol"])
            harga = int(max(cfg["base"] * 0.5, cfg["base"] + corr_effect + noise))
            records.append({
                "komoditas": nama,
                "tanggal":   r["timestamp"],
                "harga":     harga,
            })
    return records


def upload_to_hdfs(client, hdfs_dir: str, records: list[dict], filename: str):
    client.makedirs(hdfs_dir)
    path = hdfs_dir + filename
    with client.write(path, encoding="utf-8", overwrite=True) as w:
        w.write(json.dumps(records, indent=2))
    print(f"  OK {len(records)} records -> HDFS:{path}")


def main():
    print("=== Seed Data Historis ke HDFS ===")
    start = date(2026, 1, 1)
    end   = date(2026, 6, 18)

    print(f"Periode: {start} s/d {end}")

    kurs_records   = gen_kurs_data(start, end)
    pangan_records = gen_pangan_data(kurs_records)
    print(f"Data: {len(kurs_records)} kurs, {len(pangan_records)} pangan")

    os.makedirs("dashboard/data", exist_ok=True)
    live_kurs = sorted(kurs_records, key=lambda x: x["timestamp"], reverse=True)[:30]
    with open("dashboard/data/live_kurs.json", "w", encoding="utf-8") as f:
        json.dump(live_kurs, f, indent=2)

    live_pangan_by_kom = {}
    for r in sorted(pangan_records, key=lambda x: x["tanggal"], reverse=True):
        k = r["komoditas"]
        if k not in live_pangan_by_kom:
            live_pangan_by_kom[k] = r
    with open("dashboard/data/live_pangan.json", "w", encoding="utf-8") as f:
        json.dump(list(live_pangan_by_kom.values()), f, indent=2)

    print("OK Live JSON disimpan ke dashboard/data/")

    client = make_hdfs_client()
    print("Mengupload ke HDFS...")
    kurs_by_month = {}
    for r in kurs_records:
        ym = r["timestamp"][:7]
        kurs_by_month.setdefault(ym, []).append(r)
    for ym, recs in kurs_by_month.items():
        upload_to_hdfs(client, BRONZE_KURS, recs, f"kurs_{ym}.json")

    pangan_by_month = {}
    for r in pangan_records:
        ym = r["tanggal"][:7]
        pangan_by_month.setdefault(ym, []).append(r)
    for ym, recs in pangan_by_month.items():
        upload_to_hdfs(client, BRONZE_PANGAN, recs, f"pangan_{ym}.json")

    print("OK Semua data berhasil di-seed ke HDFS!")


if __name__ == "__main__":
    main()