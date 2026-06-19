import os
import json
import requests
from dotenv import load_dotenv
from urllib.parse import urlparse, urlunparse
from requests.adapters import HTTPAdapter

load_dotenv()
API_KEY = os.getenv("BPS_API_KEY")

BPS_BASE        = "https://webapi.bps.go.id/v1/api"
VAR_INFLASI     = 1       # Inflasi Bulanan (M-to-M)
VERVAR_NASIONAL = 9999    # Level INDONESIA (bukan per kota)
HDFS_BPS_DIR    = "/data/lakehouse/bronze/bps/"


class DockerHostRewrite(HTTPAdapter):
    def send(self, request, **kwargs):
        parsed = urlparse(request.url)
        if parsed.hostname not in ("localhost", "127.0.0.1", None):
            port = parsed.port
            netloc = f"localhost:{port}" if port else "localhost"
            request.url = urlunparse(parsed._replace(netloc=netloc))
        return super().send(request, **kwargs)


def get_available_years():
    url = f"{BPS_BASE}/list/model/th/domain/0000/var/{VAR_INFLASI}/key/{API_KEY}/"
    res = requests.get(url, timeout=15).json()
    years = res.get("data", [{}, []])[1]
    return [int(y["th_id"]) for y in years]


def fetch_inflasi():
    th_ids = get_available_years()
    if not th_ids:
        print("[ERROR] Tidak ada tahun tersedia untuk variabel inflasi")
        return []

    # API BPS membatasi maksimal 3 tahun per request -> ambil 3 tahun terbaru
    th_ids = sorted(th_ids, reverse=True)[:3]
    th_min, th_max = min(th_ids), max(th_ids)
    url = (
        f"{BPS_BASE}/list/model/data/domain/0000/var/{VAR_INFLASI}"
        f"/vervar/{VERVAR_NASIONAL}/th/{th_min}:{th_max}/key/{API_KEY}/"
    )
    res = requests.get(url, timeout=30).json()

    if res.get("data-availability") != "available":
        print(f"[ERROR] BPS data tidak tersedia: {res.get('data-availability')}")
        return []

    vervar_val = res["vervar"][0]["val"]
    var_val    = res["var"][0]["val"]
    turvar_val = res["turvar"][0]["val"]
    content    = res["datacontent"]

    records = []
    for th in res["tahun"]:
        for tt in res["turtahun"]:
            if tt["val"] == 13:   # skip "Tahunan" (annual summary)
                continue
            k = f"{vervar_val}{var_val}{turvar_val}{th['val']}{tt['val']}"
            if k in content:
                records.append({
                    "year": int(th["label"]),
                    "month": tt["val"],
                    "month_label": tt["label"],
                    "inflasi_mtm_persen": content[k]
                })

    records.sort(key=lambda r: (r["year"], r["month"]))
    return records


def upload_to_hdfs(records):
    try:
        from hdfs import InsecureClient
        session = requests.Session()
        session.mount("http://", DockerHostRewrite())
        client = InsecureClient("http://localhost:9870", user="root", session=session)
        client.makedirs(HDFS_BPS_DIR)
        path = HDFS_BPS_DIR + "inflasi_bulanan.json"
        with client.write(path, encoding="utf-8", overwrite=True) as w:
            w.write(json.dumps(records, indent=2, ensure_ascii=False))
        print(f"[SUCCESS] HDFS: {path}")
    except Exception as e:
        print(f"[WARN] Gagal upload ke HDFS: {e}")


def main():
    os.makedirs("../raw_data/bps", exist_ok=True)

    records = fetch_inflasi()
    if records:
        with open("../raw_data/bps/inflasi_bulanan.json", "w", encoding="utf-8") as f:
            json.dump(records, f, indent=4, ensure_ascii=False)
        print(f"[SUCCESS] Data Makro BPS ({len(records)} baris) berhasil diunduh ke raw_data/bps/inflasi_bulanan.json")
        upload_to_hdfs(records)
    else:
        print("[ERROR] Tidak ada data BPS yang berhasil diambil")

if __name__ == "__main__":
    main()