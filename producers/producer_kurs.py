"""
Producer Kurs USD-IDR
Fetch data dari BI Web Service, parse XML, dan kirim ke Kafka topic 'kurs-usd-idr'
"""

import json
import logging
from datetime import datetime
import xml.etree.ElementTree as ET
from kafka import KafkaProducer
from config.kafka_config import BOOTSTRAP_SERVERS, TOPIC_KURS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_xml_kurs(xml_data):
    """Parse XML dari BI Web Service dan ekstrak kurs USD-IDR"""
    try:
        root = ET.fromstring(xml_data)
        # Implementasi parsing sesuai format BI Web Service
        kurs_data = {
            'timestamp': datetime.now().isoformat(),
            'rate': None  # Extract dari XML
        }
        return kurs_data
    except Exception as e:
        logger.error(f"Error parsing XML: {e}")
        return None


def produce_kurs():
    """Fetch kurs dan kirim ke Kafka"""
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    try:
        # TODO: Fetch dari BI Web Service
        # xml_data = fetch_bi_webservice()
        # kurs_data = parse_xml_kurs(xml_data)
        
        kurs_data = {
            'timestamp': datetime.now().isoformat(),
            'rate': 15500.00
        }
        
        producer.send(TOPIC_KURS, value=kurs_data)
        logger.info(f"Sent kurs data: {kurs_data}")
        
    except Exception as e:
        logger.error(f"Error producing kurs: {e}")
    finally:
        producer.close()


if __name__ == "__main__":
    produce_kurs()
