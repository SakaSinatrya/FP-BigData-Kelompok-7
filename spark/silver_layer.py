import os
os.environ["HADOOP_USER_NAME"] = "root"  # Ini ID Card palsunya

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.functions import col, to_date

HDFS_BRONZE_KURS = "hdfs://localhost:8020/data/lakehouse/bronze_parquet/kurs"
HDFS_BRONZE_PANGAN = "hdfs://localhost:8020/data/lakehouse/bronze_parquet/pangan"
HDFS_SILVER_PATH = "hdfs://localhost:8020/data/lakehouse/silver"

def transform_silver():
    spark = SparkSession.builder \
        .appName("SilverLayer") \
        .master("local[*]") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()
    
    kurs_df = spark.read.parquet(HDFS_BRONZE_KURS)
    pangan_df = spark.read.parquet(HDFS_BRONZE_PANGAN)
    
    kurs_silver = kurs_df \
        .withColumn("date", to_date(col("timestamp"))) \
        .withColumn("rate", col("rate").cast(DoubleType())) \
        .select("date", "rate", "beli", "jual")
    
    pangan_silver = pangan_df \
        .withColumn("date", to_date(col("tanggal"))) \
        .withColumn("harga", col("harga").cast(IntegerType())) \
        .select("date", "komoditas", "harga")
    
    joined_df = kurs_silver.join(pangan_silver, on="date", how="inner")
    
    joined_df.write.mode("overwrite").parquet(f"{HDFS_SILVER_PATH}/kurs_pangan")
    
    print("Silver Layer Success")
    spark.stop()

if __name__ == "__main__":
    transform_silver()