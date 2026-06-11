# FP: Big Data Analytics - Kurs USD-IDR & Harga Pangan

Project ini menganalisis korelasi antara kurs USD-IDR dan harga pangan menggunakan arsitektur Big Data modern.

## 📋 Arsitektur

```
Producers → Kafka → Consumer → HDFS → Spark (Bronze/Silver/Gold) → Dashboard
```

### Komponen Utama

#### 1. **Producers** (`producers/`)
- `producer_kurs.py`: Fetch kurs USD-IDR dari BI Web Service (XML)
- `producer_pangan.py`: Scraping harga pangan dari PIHPS/SISKAPERBAPO (HTML)
- Keduanya mengirim data ke Kafka real-time

#### 2. **Apache Kafka 3.9.0** (KRaft Mode)
- **Container:** `kafka-broker`
- **Port PLAINTEXT:** `9092` (akses dari Windows)
- **Port Internal:** `29092` (antar container)
- **Topics:**
  - `kurs-usd-idr`: Streaming data kurs USD-IDR
  - `harga-pangan`: Streaming data harga pangan
- **Auto Topic Creation:** Enabled

#### 3. **Consumer** (`consumers/`)
- `consumer_to_hdfs.py`: 
  - Baca data dari Kafka topics
  - Parse JSON dan validasi
  - Simpan ke HDFS bronze layer sebagai JSON
  - Berjalan continuous (infinite loop)

#### 4. **Apache Hadoop 3** (HDFS & YARN)
- **NameNode** (`hadoop-namenode`): Port 9870 (Web UI), 8020 (RPC)
- **DataNode** (`hadoop-datanode`): Port 9864 (Web UI), 9866 (Transfer)
- **ResourceManager** (`hadoop-resourcemanager`): Port 8088 (Web UI)
- **NodeManager** (`hadoop-nodemanager`): Port 8042 (Web UI)
- **Medallion Architecture:** Bronze → Silver → Gold layers

#### 5. **Spark Layers** (`spark/`)
- `bronze_layer.py`: Load raw JSON dari HDFS, struktur tipe string
- `silver_layer.py`: Cleaning, casting tipe, join kurs + harga
- `gold_layer.py`: Agregasi bulanan, analisis korelasi Pearson

#### 6. **Dashboard** (`dashboard/`)
- Flask app di `http://localhost:5000`
- Real-time visualization kurs dan harga pangan
- API endpoints untuk data analytics

#### 7. **Config** (`config/`)
- `kafka_config.py`: Konfigurasi terpusat (Kafka, HDFS, paths)

## 🚀 Cara Menjalankan

### Prerequisites
- **Python 3.11+** (tested dengan Python 3.13)
- **Docker & Docker Compose** terinstall
- **Apache Spark** (untuk Spark submit jobs)
- **Java 11+** (untuk Spark & Hadoop)

---

### 📍 Langkah 1: Aktifkan Virtual Environment

#### Windows (PowerShell):
```powershell
.\venv\Scripts\activate
```

#### Windows (CMD):
```cmd
.\venv\Scripts\activate.bat
```

#### macOS/Linux:
```bash
source venv/bin/activate
```

✅ **Indikator sukses:** Muncul tanda `(venv)` di sebelah kiri prompt terminal Anda

---

### 📍 Langkah 2: Instalasi Dependensi Proyek

Setelah virtual environment aktif, pasang semua library Python dari `requirements.txt`:

```bash
pip install -r requirements.txt
```

**Keterangan dependensi:**
- `kafka-python-ng`: Producer & Consumer Kafka (compatible Python 3.13)
- `hdfs`: WebHDFS client untuk Consumer
- `pyspark`, `delta-spark`: Spark processing & Delta Lake
- `Flask`: Web dashboard
- `requests`, `beautifulsoup4`: Data sourcing (API & scraping)

---

### 📍 Langkah 3: Membersihkan Container Lama (Jika Ada)

Jika ada container dari proyek sebelumnya, bersihkan terlebih dahulu:

```powershell
docker rm -f kafka-broker hadoop-namenode hadoop-datanode hadoop-resourcemanager hadoop-nodemanager
```

---

### 📍 Langkah 4: Jalankan Infrastruktur Big Data (Docker)

Pastikan file `docker-compose.yml` dan `hadoop.env` berada di direktori root proyek.

Jalankan semua container dalam mode background (*detached*):

```bash
docker-compose up -d
```

**⚠️ PENTING:** Tunggu **20-30 detik** sebelum melanjutkan ke langkah berikutnya agar:
- Kafka KRaft controller dan broker selesai booting
- Hadoop NameNode selesai formatting
- HDFS fully operational

---

### 📍 Langkah 5: Konfigurasi Izin Akses HDFS

Atur permission root HDFS agar script Consumer Python dapat menulis data secara dinamis:

```powershell
docker exec -it hadoop-namenode hdfs dfs -chmod 777 /
```

Verifikasi permission berhasil diatur:
```powershell
docker exec -it hadoop-namenode hdfs dfs -ls -d /
```

**Output yang diharapkan:** `drwxrwxrwx ...` (755 atau 777)

---

### 📍 Langkah 6: Verifikasi Infrastruktur

Pastikan semua service sudah berjalan:

```bash
docker-compose ps
```

**Output yang diharapkan:**
| Service | Status |
|---------|--------|
| kafka-broker | running |
| hadoop-namenode | running |
| hadoop-datanode | running |
| hadoop-resourcemanager | running |
| hadoop-nodemanager | running |

Akses Web UI untuk verifikasi:
- **Kafka KRaft:** `http://localhost:9092` (port PLAINTEXT_HOST)
- **NameNode Web UI:** `http://localhost:9870`
- **ResourceManager Web UI:** `http://localhost:8088`

---

### 📍 Langkah 7: Run Producers

Buka terminal baru (tetap aktifkan venv), jalankan producer:

**Terminal 1 - Producer Kurs USD-IDR:**
```bash
python -m producers.producer_kurs
```

**Terminal 2 - Producer Harga Pangan:**
```bash
python -m producers.producer_pangan
```

Verifikasi data masuk ke Kafka:
```powershell
docker exec -it kafka-broker kafka-console-consumer --bootstrap-server localhost:9092 --topic kurs-usd-idr --from-beginning --max-messages 5
```

---

### 📍 Langkah 8: Run Consumer to HDFS

Terminal baru, jalankan consumer:

```bash
python -m consumers.consumer_to_hdfs
```

**Apa yang terjadi:**
- Consumer membaca data dari Kafka topics
- Menyimpan sebagai JSON ke HDFS bronze layer (`/data/lakehouse/bronze/`)
- Berjalan continuous (tekan Ctrl+C untuk stop)

Verifikasi data di HDFS:
```powershell
docker exec -it hadoop-namenode hdfs dfs -ls -R /data/lakehouse/bronze/
```

---

### 📍 Langkah 9: Run Spark Processing Layers

Jalankan Spark jobs untuk transformasi data. Pastikan Consumer sudah berjalan dan data tersimpan di HDFS.

**Terminal baru:**

```bash
# 1. Bronze Layer (Load raw data)
spark-submit spark/bronze_layer.py

# 2. Silver Layer (Clean & transform)
spark-submit spark/silver_layer.py

# 3. Gold Layer (Aggregate & analytics)
spark-submit spark/gold_layer.py
```

**Catatan:** Setiap layer perlu menunggu layer sebelumnya selesai.

Monitor progress di Spark Web UI: `http://localhost:4040`

---

### 📍 Langkah 10: Start Dashboard

Terminal baru, jalankan Flask dashboard:

```bash
cd dashboard
python app.py
```

**Output:**
```
 * Running on http://127.0.0.1:5000
 * Debug mode: ON
```

Buka browser ke `http://localhost:5000` untuk visualisasi real-time data kurs dan harga pangan.

---

## 📊 Data Flow

```
┌──────────────────────────────────────────────────────────────────────────┐
│                        BIG DATA PIPELINE FLOW                            │
└──────────────────────────────────────────────────────────────────────────┘

1. SOURCING (Data Ingestion)
   ├─→ Producer Kurs: BI Web Service (XML) → Kafka Topic (kurs-usd-idr)
   └─→ Producer Pangan: Web Scraping (HTML) → Kafka Topic (harga-pangan)

2. STREAMING (Kafka Message Broker)
   ├─→ Topic: kurs-usd-idr (JSON format)
   └─→ Topic: harga-pangan (JSON format)

3. BRONZE LAYER (Raw Data Storage)
   ├─→ Consumer reads Kafka → HDFS /data/lakehouse/bronze/kurs/ (JSON)
   └─→ Consumer reads Kafka → HDFS /data/lakehouse/bronze/pangan/ (JSON)

4. SILVER LAYER (Cleaned & Typed Data)
   ├─→ bronze_layer.py: Load raw JSON
   ├─→ silver_layer.py: 
   │   ├─ Type casting (DATE, INT, FLOAT)
   │   ├─ Data cleaning & validation
   │   └─ → HDFS /data/lakehouse/silver/ (Parquet)

5. GOLD LAYER (Analytics & Aggregation)
   ├─→ gold_layer.py:
   │   ├─ Join kurs + pangan by date
   │   ├─ Monthly aggregation
   │   ├─ Correlation analysis (Pearson)
   │   └─ → HDFS /data/lakehouse/gold/dampak_kurs_pangan/ (Parquet)

6. VISUALIZATION & BI
   ├─→ Dashboard (Flask):
   │   ├─ GET /api/kurs: Latest kurs + 7-day history
   │   ├─ GET /api/pangan: Latest harga pangan
   │   └─ GET /api/correlation: Pearson correlation results
   └─→ Web UI: http://localhost:5000
```

**Timeline Processing:**
- **Real-time:** Producers → Kafka (continuous)
- **Continuous:** Consumer → HDFS Bronze (24/7)
- **Scheduled:** Spark layers (recommended: daily/hourly)
- **On-demand:** Dashboard queries Gold layer


## 🔧 Konfigurasi

Konfigurasi terpusat disimpan di `config/kafka_config.py`:

### Kafka Configuration
```python
KAFKA_BROKER = ['localhost:9092']           # PLAINTEXT_HOST (akses dari Windows)
KAFKA_API_VERSION = (3, 9, 0)              # KRaft Mode
TOPIC_KURS = 'kurs-usd-idr'
TOPIC_PANGAN = 'harga-pangan'
```

### HDFS Configuration
```python
HDFS_WEB_URL = 'http://localhost:9870'     # WebHDFS (Consumer)
HDFS_RPC_URL = 'hdfs://localhost:8020'     # NameNode RPC (Spark)
HDFS_USER = 'root'
```

### HDFS Paths (Medallion Architecture)
```python
BRONZE_KURS_PATH = '/data/lakehouse/bronze/kurs/'
BRONZE_PANGAN_PATH = '/data/lakehouse/bronze/pangan/'

SILVER_KURS_PATH = '/data/lakehouse/silver/kurs/'
SILVER_PANGAN_PATH = '/data/lakehouse/silver/pangan/'

GOLD_ANALYTICS_PATH = '/data/lakehouse/gold/dampak_kurs_pangan/'
```

### Service Ports
| Service | Port | URL |
|---------|------|-----|
| Kafka PLAINTEXT | 9092 | `localhost:9092` |
| NameNode Web UI | 9870 | `http://localhost:9870` |
| NameNode RPC | 8020 | `hdfs://localhost:8020` |
| DataNode Web UI | 9864 | `http://localhost:9864` |
| DataNode Transfer | 9866 | `localhost:9866` |
| ResourceManager Web UI | 8088 | `http://localhost:8088` |
| NodeManager Web UI | 8042 | `http://localhost:8042` |
| Dashboard Flask | 5000 | `http://localhost:5000` |


