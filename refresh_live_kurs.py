"""
Refresh dashboard/data/live_kurs.json dengan data ASLI dari BI Web Service.
Dipakai karena seed_hdfs_data.py (simulasi) menimpa file live dengan
angka random-walk yang tidak merepresentasikan kurs sungguhan.
"""
import json
import requests
import xml.etree.ElementTree as ET
from datetime import date, timedelta

def fetch_kurs() -> list[dict]:
    end = date.today()
    start = end - timedelta(days=30)

    url = f"https://www.bi.go.id/biwebservice/wskursbi.asmx/getSubKursLokal3?mts=USD&startdate={start}&enddate={end}"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "id-ID,id;q=0.9,en;q=0.8",
        "Referer": "https://www.bi.go.id/",
    }

    try:
        res = requests.get(url, timeout=30, headers=headers, verify=False)
        root = ET.fromstring(res.text)
        records = []

        for row in root.findall(".//{http://www.bi.go.id/}Table") or root.findall(".//Table"):
            tgl = row.findtext(".//{http://www.bi.go.id/}tgl_subkurslokal") or row.findtext("tgl_subkurslokal")
            beli = row.findtext(".//{http://www.bi.go.id/}beli_subkurslokal") or row.findtext("beli_subkurslokal")
            jual = row.findtext(".//{http://www.bi.go.id/}jual_subkurslokal") or row.findtext("jual_subkurslokal")

            if tgl and beli and jual:
                b, j = float(beli), float(jual)
                records.append({
                    "timestamp": tgl.split('T')[0],
                    "rate": (b + j) / 2,
                    "beli": b,
                    "jual": j
                })
        return records
    except Exception as e:
        print(f"[ERROR] Kurs Fetch: {e}")
        return []


records = fetch_kurs()
print(f"Fetch {len(records)} record kurs ASLI dari BI")

if records:
    records_sorted = sorted(records, key=lambda r: r["timestamp"], reverse=True)
    with open("dashboard/data/live_kurs.json", "w") as f:
        json.dump(records_sorted, f, indent=2)
    print(f"Terbaru: {records_sorted[0]}")
    print("dashboard/data/live_kurs.json diperbarui dengan data ASLI.")
else:
    print("Gagal fetch - cek koneksi ke bi.go.id")