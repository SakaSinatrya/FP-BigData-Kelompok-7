# config/kafka_config.py

# KONFIGURASI KAFKA BROKER
# Alamat broker untuk akses dari luar Docker (Windows) sesuai PLAINTEXT_HOST
KAFKA_BROKER = ['localhost:9092']

# Sangat penting untuk stabilitas kafka-python-ng di Python 3.13
KAFKA_API_VERSION = (3, 9, 0)

# KONFIGURASI TOPIK KAFKA
# Nama topik untuk aliran data mentah
TOPIC_KURS = 'kurs-usd-idr'
TOPIC_PANGAN = 'harga-pangan'

# KONFIGURASI HADOOP / HDFS
# URL WebHDFS untuk operasi baca/tulis melalui library Python 'hdfs' (Komponen Consumer)
HDFS_WEB_URL = 'http://localhost:9870'

# URL RPC yang akan dipanggil oleh PySpark saat pemrosesan data (Komponen Spark)
HDFS_RPC_URL = 'hdfs://localhost:8020'

# User HDFS untuk menghindari isu 'Permission Denied' saat script menulis data
HDFS_USER = 'root'

# PATH DIREKTORI DATA LAKEHOUSE (MEDALLION)
# 1. BRONZE LAYER (Penyimpanan Raw Data JSON dari Kafka)
BRONZE_KURS_PATH = '/data/lakehouse/bronze/kurs/'
BRONZE_PANGAN_PATH = '/data/lakehouse/bronze/pangan/'

# 2. SILVER LAYER (Penyimpanan Data Cleaned berformat Parquet)
SILVER_KURS_PATH = '/data/lakehouse/silver/kurs/'
SILVER_PANGAN_PATH = '/data/lakehouse/silver/pangan/'

# 3. GOLD LAYER (Penyimpanan Data Agregasi/Join siap pakai untuk Dashboard)
GOLD_ANALYTICS_PATH = '/data/lakehouse/gold/dampak_kurs_pangan/'