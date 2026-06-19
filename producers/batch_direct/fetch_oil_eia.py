import os
import json
import requests
from datetime import date, timedelta
from dotenv import load_dotenv
from urllib.parse import urlparse, urlunparse
from requests.adapters import HTTPAdapter

load_dotenv()
API_KEY = os.getenv("EIA_API_KEY")

HDFS_EIA_DIR = "/data/lakehouse/bronze/eia/"


class DockerHostRewrite(HTTPAdapter):
    def send(self, request, **kwargs):
        parsed = urlparse(request.url)
        if parsed.hostname not in ("localhost", "127.0.0.1", None):
            port = parsed.port
            netloc = f"localhost:{port}" if port else "localhost"
            request.url = urlunparse(parsed._replace(netloc=netloc))
        return super().send(request, **kwargs)


def upload_to_hdfs(records):
    try:
        from hdfs import InsecureClient
        session = requests.Session()
        session.mount("http://", DockerHostRewrite())
        client = InsecureClient("http://localhost:9870", user="root", session=session)
        client.makedirs(HDFS_EIA_DIR)
        path = HDFS_EIA_DIR + "oil_prices.json"
        with client.write(path, encoding="utf-8", overwrite=True) as w:
            w.write(json.dumps(records, indent=2))
        print(f"[SUCCESS] HDFS: {path}")
    except Exception as e:
        print(f"[WARN] Gagal upload ke HDFS: {e}")


def main():
    os.makedirs("../raw_data/eia", exist_ok=True)

    end_date = date.today()
    start_date = end_date - timedelta(days=90)

    URL = f"https://api.eia.gov/v2/petroleum/pri/spt/data/?frequency=daily&data[0]=value&facets[series][]=RBRTE&start={start_date}&end={end_date}&api_key={API_KEY}"

    try:
        response = requests.get(URL, timeout=30)
        response.raise_for_status()
        raw = response.json()

        with open("../raw_data/eia/oil_prices.json", "w") as f:
            json.dump(raw, f, indent=4)

        print(f"[SUCCESS] Data Minyak Mentah EIA ({start_date} s/d {end_date}) berhasil diunduh ke raw_data/eia/oil_prices.json")

        # Bentuk ulang jadi format simpel: {date, price_usd} per baris, lalu upload ke HDFS Bronze
        rows = raw.get("response", {}).get("data", [])
        records = [
            {"date": r["period"], "price_usd": float(r["value"]), "unit": r.get("units", "$/BBL")}
            for r in rows
        ]
        records.sort(key=lambda x: x["date"])

        if records:
            upload_to_hdfs(records)
        else:
            print("[WARN] Tidak ada data EIA untuk diupload ke HDFS")

    except Exception as e:
        print(f"[ERROR] Gagal mengunduh data EIA: {e}")

if __name__ == "__main__":
    main()