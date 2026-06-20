import os
os.environ["HADOOP_USER_NAME"] = "root"

from pyspark.sql import SparkSession

HDFS_RAW_KURS   = "hdfs://localhost:8020/data/lakehouse/bronze/kurs"
HDFS_RAW_PANGAN = "hdfs://localhost:8020/data/lakehouse/bronze/pangan"
HDFS_RAW_EIA    = "hdfs://localhost:8020/data/lakehouse/bronze/eia"
HDFS_RAW_BPS    = "hdfs://localhost:8020/data/lakehouse/bronze/bps"

HDFS_BRONZE_KURS   = "hdfs://localhost:8020/data/lakehouse/bronze_parquet/kurs"
HDFS_BRONZE_PANGAN = "hdfs://localhost:8020/data/lakehouse/bronze_parquet/pangan"
HDFS_BRONZE_EIA    = "hdfs://localhost:8020/data/lakehouse/bronze_parquet/eia"
HDFS_BRONZE_BPS    = "hdfs://localhost:8020/data/lakehouse/bronze_parquet/bps"


def load_raw_data():
    spark = SparkSession.builder \
        .appName("BronzeLayer") \
        .master("local[*]") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Ini dia "gunting"-nya biar kokinya bisa baca data yang berlapis-lapis
    try:
        kurs_df = spark.read.option("multiline", "true").json(f"{HDFS_RAW_KURS}/*")
        kurs_df.write.mode("overwrite").parquet(HDFS_BRONZE_KURS)
    except Exception as e:
        print(f"Skipping Kurs: {e}")

    try:
        pangan_df = spark.read.option("multiline", "true").json(f"{HDFS_RAW_PANGAN}/*")
        pangan_df.write.mode("overwrite").parquet(HDFS_BRONZE_PANGAN)
    except Exception as e:
        print(f"Skipping Pangan: {e}")

    try:
        eia_df = spark.read.option("multiline", "true").json(f"{HDFS_RAW_EIA}/*")
        eia_df.write.mode("overwrite").parquet(HDFS_BRONZE_EIA)
    except Exception as e:
        print(f"Skipping EIA: Data tidak ditemukan")
        
    try:
        bps_df = spark.read.option("multiline", "true").json(f"{HDFS_RAW_BPS}/*")
        bps_df.write.mode("overwrite").parquet(HDFS_BRONZE_BPS)
    except Exception as e:
        print(f"Skipping BPS: Data tidak ditemukan")

    print("Tugas Bronze Layer berhasil! (kurs, pangan, eia, bps)")
    spark.stop()


if __name__ == "__main__":
    load_raw_data()