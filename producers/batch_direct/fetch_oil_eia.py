import os
import json
import requests
from datetime import date, timedelta
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("EIA_API_KEY")

def main():
    os.makedirs("../raw_data/eia", exist_ok=True)
    
    end_date = date.today()
    start_date = end_date - timedelta(days=30)
    
    URL = f"https://api.eia.gov/v2/petroleum/pri/spt/data/?frequency=daily&data[0]=value&facets[series][]=RBRTE&start={start_date}&end={end_date}&api_key={API_KEY}"
    
    try:
        response = requests.get(URL, timeout=30)
        response.raise_for_status()
        
        with open("../raw_data/eia/oil_prices.json", "w") as f:
            json.dump(response.json(), f, indent=4)
            
        print(f"[SUCCESS] Data Minyak Mentah EIA ({start_date} s/d {end_date}) berhasil diunduh ke raw_data/eia/oil_prices.json")
    except Exception as e:
        print(f"[ERROR] Gagal mengunduh data EIA: {e}")

if __name__ == "__main__":
    main()
