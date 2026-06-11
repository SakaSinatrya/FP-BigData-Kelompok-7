"""
Silver Layer
Cleaning, casting tipe data, join data kurs dan harga pangan
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, DateType
from pyspark.sql.functions import col, to_date, round

HDFS_BRONZE_PATH = "hdfs://namenode:8020/data/bronze"
HDFS_SILVER_PATH = "hdfs://namenode:8020/data/silver"


def transform_silver():
    """Transform bronze ke silver dengan cleaning dan casting"""
    spark = SparkSession.builder \
        .appName("SilverLayer") \
        .master("spark://spark-master:7077") \
        .getOrCreate()
    
    # Load bronze data
    kurs_df = spark.read.parquet(f"{HDFS_BRONZE_PATH}/kurs")
    pangan_df = spark.read.parquet(f"{HDFS_BRONZE_PATH}/pangan")
    
    # Transform kurs: cast to double, parse timestamp
    kurs_silver = kurs_df \
        .withColumn("rate", col("rate").cast(DoubleType())) \
        .withColumn("date", to_date(col("timestamp"))) \
        .select("date", "rate")
    
    # Transform pangan: parse timestamp
    pangan_silver = pangan_df \
        .withColumn("date", to_date(col("timestamp"))) \
        .select("date", "items")
    
    # Join kurs dan pangan berdasarkan date
    joined_df = kurs_silver.join(
        pangan_silver,
        on="date",
        how="inner"
    )
    
    # Save ke silver
    joined_df.write.mode("overwrite").parquet(f"{HDFS_SILVER_PATH}/kurs_pangan")
    
    print("Silver layer created successfully")
    spark.stop()


if __name__ == "__main__":
    transform_silver()
