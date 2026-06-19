import os
os.environ["HADOOP_USER_NAME"] = "root"

from pyspark.sql import SparkSession

HDFS_RAW_KURS = "hdfs://localhost:8020/data/lakehouse/bronze/kurs"
HDFS_RAW_PANGAN = "hdfs://localhost:8020/data/lakehouse/bronze/pangan"
HDFS_BRONZE_KURS = "hdfs://localhost:8020/data/lakehouse/bronze_parquet/kurs"
HDFS_BRONZE_PANGAN = "hdfs://localhost:8020/data/lakehouse/bronze_parquet/pangan"

def load_raw_data():
    spark = SparkSession.builder \
        .appName("BronzeLayer") \
        .master("local[*]") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()
    
    # Ini dia "gunting"-nya biar kokinya bisa baca data yang berlapis-lapis
    kurs_df = spark.read.option("multiline", "true").json(f"{HDFS_RAW_KURS}/*")
    pangan_df = spark.read.option("multiline", "true").json(f"{HDFS_RAW_PANGAN}/*")
    
    kurs_df.write.mode("overwrite").parquet(HDFS_BRONZE_KURS)
    pangan_df.write.mode("overwrite").parquet(HDFS_BRONZE_PANGAN)
    
    print("Tugas Bronze Layer berhasil!")
    spark.stop()

if __name__ == "__main__":
    load_raw_data()