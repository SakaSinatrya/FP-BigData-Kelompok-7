"""
Gold Layer
Agregasi data bulanan, korelasi kurs vs harga pangan
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, avg, corr, when
from pyspark.sql.window import Window

HDFS_SILVER_PATH = "hdfs://namenode:8020/data/silver"
HDFS_GOLD_PATH = "hdfs://namenode:8020/data/gold"


def create_gold_layer():
    """Create gold layer dengan agregasi bulanan dan korelasi"""
    spark = SparkSession.builder \
        .appName("GoldLayer") \
        .master("spark://spark-master:7077") \
        .getOrCreate()
    
    # Load silver data
    silver_df = spark.read.parquet(f"{HDFS_SILVER_PATH}/kurs_pangan")
    
    # Extract tahun dan bulan
    silver_df = silver_df \
        .withColumn("year", year(col("date"))) \
        .withColumn("month", month(col("date")))
    
    # Agregasi bulanan - rata-rata kurs
    monthly_kurs = silver_df \
        .groupBy("year", "month") \
        .agg(avg("rate").alias("avg_kurs_rate")) \
        .orderBy("year", "month")
    
    # TODO: Agregasi bulanan untuk harga pangan (perlu parsing 'items' field)
    # monthly_pangan = ...
    
    # Korelasi kurs vs harga (contoh)
    window_spec = Window.partitionBy("year")
    correlation_df = silver_df \
        .withColumn("correlation", corr("rate", col("date")).over(window_spec)) \
        .select("year", "correlation") \
        .distinct()
    
    # Save ke gold
    monthly_kurs.write.mode("overwrite").parquet(f"{HDFS_GOLD_PATH}/monthly_kurs")
    correlation_df.write.mode("overwrite").parquet(f"{HDFS_GOLD_PATH}/correlation")
    
    print("Gold layer created successfully")
    spark.stop()


if __name__ == "__main__":
    create_gold_layer()
