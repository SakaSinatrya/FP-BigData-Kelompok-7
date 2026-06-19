import os, sys, time, json, requests
import xml.etree.ElementTree as ET
from datetime import date, timedelta
from kafka import KafkaProducer

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.kafka_config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_KURS

def fetch_kurs() -> list[dict]:
    end = date.today()
    start = end - timedelta(days=30)
    
    url = f"https://www.bi.go.id/biwebservice/wskursbi.asmx/getSubKursLokal3?mts=USD&startdate={start}&enddate={end}"
    
    try:
        res = requests.get(url, timeout=15)
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
                    "rate": (b+j)/2, 
                    "beli": b, 
                    "jual": j
                })
        return records
    except Exception as e:
        print(f"[ERROR] Kurs Fetch: {e}")
        return []

def main():
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, 
                             value_serializer=lambda v: json.dumps(v).encode("utf-8"))
    print(f"Producer Kurs Aktif. Target topic: {TOPIC_KURS}")
    
    try:
        while True:
            for rec in fetch_kurs():
                producer.send(TOPIC_KURS, value=rec)
                print(f"[KURS] Sent: {rec}")
            
            producer.flush()
            time.sleep(3600)
            
    except KeyboardInterrupt:
        producer.close()
        print("Producer Kurs Dimatikan.")

if __name__ == "__main__":
    main()
