import os, sys, time, json
from datetime import datetime
from kafka import KafkaProducer
from curl_cffi import requests 

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.kafka_config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_PANGAN

API_URL = "https://www.bi.go.id/hargapangan/Website/Home/GetDetailGridData2"
KOMODITAS = {
    "12": "Bawang Putih Impor", 
    "16": "Cabai Rawit Lokal", 
    "3": "Beras Medium"
}

def main():
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, 
                             value_serializer=lambda v: json.dumps(v).encode("utf-8"))
    
    session = requests.Session()
    session.get("https://www.bi.go.id/hargapangan", impersonate="chrome120", verify=False) 
    
    print(f"Producer Pangan Aktif. Target topic: {TOPIC_PANGAN}")
    
    try:
        while True:
            tgl_kunci = datetime.now().strftime("%d/%m/%Y")
            iso_date = datetime.now().date().isoformat()
            
            for cid, cname in KOMODITAS.items():
                params = {"ProvId": "0", "PriceTypeId": "1", "ComId": cid, "date": tgl_kunci.replace("/"," "), "isPasokan": "1", "_": int(time.time()*1000)}
                try:
                    res = session.get(API_URL, params=params, impersonate="chrome120", verify=False).json()
                    
                    for row in res.get("data", []):
                        if row.get("name") == "Semua Provinsi" and row.get(tgl_kunci):
                            rec = {"komoditas": cname, "tanggal": iso_date, "harga": int(float(row[tgl_kunci]))}
                            producer.send(TOPIC_PANGAN, value=rec)
                            print(f"[PANGAN] Sent: {rec}")
                            break
                except Exception as e:
                    print(f"[ERROR] Fetch {cname}: {e}")
                
                time.sleep(2) 
            
            producer.flush()
            time.sleep(3600) 
            
    except KeyboardInterrupt:
        producer.close()
        print("Producer Pangan Dimatikan.")

if __name__ == "__main__":
    main()
