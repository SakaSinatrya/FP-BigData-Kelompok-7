import os
os.environ["HADOOP_USER_NAME"] = "root"  # Ini ID Card palsunya

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.functions import col, to_date

HDFS_BRONZE_KURS   = "hdfs://localhost:8020/data/lakehouse/bronze_parquet/kurs"
HDFS_BRONZE_PANGAN = "hdfs://localhost:8020/data/lakehouse/bronze_parquet/pangan"
HDFS_BRONZE_EIA    = "hdfs://localhost:8020/data/lakehouse/bronze_parquet/eia"
HDFS_SILVER_PATH   = "hdfs://localhost:8020/data/lakehouse/silver"


def transform_silver():
    spark = SparkSession.builder \
        .appName("SilverLayer") \
        .master("local[*]") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    kurs_df   = spark.read.parquet(HDFS_BRONZE_KURS)
    pangan_df = spark.read.parquet(HDFS_BRONZE_PANGAN)
    kurs_silver = kurs_df \
        .withColumn("date", to_date(col("timestamp"))) \
        .withColumn("rate", col("rate").cast(DoubleType())) \
        .select("date", "rate", "beli", "jual")

    pangan_silver = pangan_df \
        .withColumn("date", to_date(col("tanggal"))) \
        .withColumn("harga", col("harga").cast(IntegerType())) \
        .select("date", "komoditas", "harga")

    # JOIN kurs + pangan (harian, inner - keduanya wajib ada di hari yang sama)
    joined_df = kurs_silver.join(pangan_silver, on="date", how="inner")

    try:
        eia_df = spark.read.parquet(HDFS_BRONZE_EIA)
        eia_silver = eia_df \
            .withColumn("date", to_date(col("date"))) \
            .withColumn("oil_price_usd", col("price_usd").cast(DoubleType())) \
            .select("date", "oil_price_usd")
        
        # LEFT JOIN harga minyak Brent (EIA hanya tersedia di hari bursa,
        # sehingga beberapa baris akan punya oil_price_usd = null)
        joined_df = joined_df.join(eia_silver, on="date", how="left")
    except Exception as e:
        print(f"Skipping EIA in Silver: {e}")
        from pyspark.sql.functions import lit
        joined_df = joined_df.withColumn("oil_price_usd", lit(None).cast(DoubleType()))

    joined_df.write.mode("overwrite").parquet(f"{HDFS_SILVER_PATH}/kurs_pangan")

    print("Silver Layer Success (kurs + pangan + oil)")
    spark.stop()


if __name__ == "__main__":
    transform_silver()