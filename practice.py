from pyspark.sql import SparkSession
import os

os.environ["HADOOP_HOME"] = "C:/hadoop"

spark = SparkSession.builder \
    .appName("Practice") \
    .master("local[*]") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark 켜졌다!")

# Kafka에서 데이터 읽기
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:29092") \
    .option("subscribe", "upbit-trades") \
    .option("startingOffsets", "latest") \
    .load()

print("Kafka 연결됐다!")
print("컬럼 목록:", raw_df.columns)