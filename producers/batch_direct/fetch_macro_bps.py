import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("BPS_API_KEY")

URL = f"https://webapi.bps.go.id/v1/api/list/model/data/domain/0000/var/1/key/{API_KEY}/"

def main():
    os.makedirs("../raw_data/bps", exist_ok=True)
    
    try:
        response = requests.get(URL, timeout=30)
        response.raise_for_status()
        
        with open("../raw_data/bps/inflasi_bulanan.json", "w") as f:
            json.dump(response.json(), f, indent=4)
            
        print("[SUCCESS] Data Makro BPS berhasil diunduh ke raw_data/bps/inflasi_bulanan.json")
    except Exception as e:
        print(f"[ERROR] Gagal mengunduh data BPS: {e}")

if __name__ == "__main__":
    main()
