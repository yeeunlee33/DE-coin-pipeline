from pyspark.sql import SparkSession
import os

print("1. Spark 생성 시작")

# Hadoop 경로 설정
os.environ["HADOOP_HOME"] = "C:/hadoop"

# SparkSession 생성
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .master("local[*]") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
    ) \
    .config(
        "spark.sql.streaming.checkpointLocation",
        "C:/tmp/spark-checkpoint"
    ) \
    .getOrCreate()

print("2. Spark 생성 완료")

# Kafka Stream 읽기
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("subscribe", "test-topic") \
    .load()

print("3. Kafka 연결 성공")

# 콘솔 출력 스트리밍 시작
query = df.selectExpr("CAST(value AS STRING)") \
    .writeStream \
    .format("console") \
    .start()

print("4. 스트리밍 시작")

# 스트리밍 유지
query.awaitTermination()