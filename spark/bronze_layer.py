"""
Bronze Layer
Load raw data dari HDFS, simpan dengan struktur tipe string
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

HDFS_RAW_PATH = "hdfs://namenode:8020/data/raw"
HDFS_BRONZE_PATH = "hdfs://namenode:8020/data/bronze"


def load_raw_data():
    """Load raw data dari HDFS"""
    spark = SparkSession.builder \
        .appName("BronzeLayer") \
        .master("spark://spark-master:7077") \
        .getOrCreate()
    
    # Schema untuk bronze layer (semua string)
    bronze_schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("data", StringType(), True),
        StructField("source", StringType(), True)
    ])
    
    # Load kurs data
    kurs_df = spark.read.json(f"{HDFS_RAW_PATH}/kurs/*")
    kurs_df = kurs_df.select(
        "timestamp",
        "rate"
    ).withColumn("rate", kurs_df.rate.cast(StringType()))
    
    # Load pangan data
    pangan_df = spark.read.json(f"{HDFS_RAW_PATH}/pangan/*")
    
    # Save ke bronze
    kurs_df.write.mode("overwrite").parquet(f"{HDFS_BRONZE_PATH}/kurs")
    pangan_df.write.mode("overwrite").parquet(f"{HDFS_BRONZE_PATH}/pangan")
    
    print("Bronze layer loaded successfully")
    spark.stop()


if __name__ == "__main__":
    load_raw_data()
