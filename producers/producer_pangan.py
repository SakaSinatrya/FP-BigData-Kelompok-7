"""
Producer Harga Pangan
Scraping data dari PIHPS/SISKAPERBAPO, parse HTML, dan kirim ke Kafka topic 'harga-pangan'
"""

import json
import logging
from datetime import datetime
from kafka import KafkaProducer
from config.kafka_config import BOOTSTRAP_SERVERS, TOPIC_PANGAN

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def scrape_pangan_data():
    """Scraping harga pangan dari PIHPS/SISKAPERBAPO"""
    try:
        # TODO: Implementasi web scraping
        # Bisa menggunakan BeautifulSoup atau Selenium
        pangan_data = {
            'timestamp': datetime.now().isoformat(),
            'items': [
                {'name': 'Beras Premium', 'price': 12000},
                {'name': 'Minyak Goreng', 'price': 18000}
            ]
        }
        return pangan_data
    except Exception as e:
        logger.error(f"Error scraping pangan: {e}")
        return None


def produce_pangan():
    """Scrape dan kirim data harga pangan ke Kafka"""
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    try:
        pangan_data = scrape_pangan_data()
        if pangan_data:
            producer.send(TOPIC_PANGAN, value=pangan_data)
            logger.info(f"Sent pangan data: {pangan_data}")
        
    except Exception as e:
        logger.error(f"Error producing pangan: {e}")
    finally:
        producer.close()


if __name__ == "__main__":
    produce_pangan()
