import os, sys, time, json
from datetime import datetime
from kafka import KafkaProducer
import requests
from bs4 import BeautifulSoup
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from config.kafka_config import KAFKA_BROKER, TOPIC_PANGAN

API_URL = "https://siskaperbapo.jatimprov.go.id/"

KOMODITAS_MAP = {
    "Bawang Putih / kg": "Bawang Putih Impor", 
    "Cabe Rawit Merah / kg": "Cabai Rawit Lokal", 
    "Beras Medium / kg": "Beras Medium"
}

def fetch_siskaperbapo():
    try:
        res = requests.get(API_URL, verify=False, timeout=15)
        soup = BeautifulSoup(res.text, "html.parser")
        
        records = []
        iso_date = datetime.now().date().isoformat()
        
        for tr in soup.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) >= 4:
                nama_asli = tds[1].text.strip()
                harga_str = tds[3].text.strip()
                
                if nama_asli in KOMODITAS_MAP and harga_str:
                    cname = KOMODITAS_MAP[nama_asli]
                    harga_clean = int(harga_str.replace(".", ""))
                    
                    records.append({
                        "komoditas": cname,
                        "tanggal": iso_date,
                        "harga": harga_clean
                    })
                    
        return records
    except Exception as e:
        print(f"[ERROR] Fetch Siskaperbapo: {e}")
        return []

def main():
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER, 
                             value_serializer=lambda v: json.dumps(v).encode("utf-8"))
    
    print(f"Producer Pangan (Siskaperbapo) Aktif. Target topic: {TOPIC_PANGAN}")
    
    try:
        while True:
            records = fetch_siskaperbapo()
            for rec in records:
                producer.send(TOPIC_PANGAN, value=rec)
                print(f"[PANGAN] Sent: {rec}")
                
            producer.flush()
            time.sleep(3600) 
            
    except KeyboardInterrupt:
        producer.close()
        print("Producer Pangan Dimatikan.")

if __name__ == "__main__":
    main()
