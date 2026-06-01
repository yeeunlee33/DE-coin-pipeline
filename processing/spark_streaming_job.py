import os
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime

from schemas import TRADE_SCHEMA
from window_aggregation import aggregate_ohlcv
from anomaly_detection import detect_anomaly

# ── 환경 설정 ──────────────────────────────────────────────
os.environ["HADOOP_HOME"] = "C:/hadoop"

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:29092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "upbit-trades")
CHECKPOINT_DIR          = os.getenv("CHECKPOINT_DIR", "C:/tmp/spark-checkpoint")

# ── PostgreSQL 설정 ────────────────────────────────────────
PG_URL  = "jdbc:postgresql://localhost:5432/coindb"
PG_PROPS = {
    "user":   "postgres",
    "password": "password",
    "driver": "org.postgresql.Driver",
}
PG_CONN = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "coindb",
    "user":     "postgres",
    "password": "password",
}


def create_spark_session() -> SparkSession:
    return SparkSession.builder \
        .appName("CoinStreamingJob") \
        .master("local[*]") \
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "org.postgresql:postgresql:42.7.3"  # PostgreSQL JDBC 드라이버
        ) \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
        .getOrCreate()


def read_from_kafka(spark: SparkSession):
    """Kafka에서 raw 데이터 읽기"""
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()


def parse_kafka_data(raw_df):
    """
    Kafka raw 데이터 파싱
    Kafka value는 bytes로 오기 때문에 문자열 → JSON으로 변환
    """
    return raw_df \
        .select(
            from_json(
                col("value").cast("string"),
                TRADE_SCHEMA
            ).alias("data")
        ) \
        .select("data.*") \
        .withColumn(
            # timestamp(ms) → 실제 시각으로 변환 (Spark 윈도우 연산에 필요)
            "event_time",
            from_unixtime(col("timestamp") / 1000).cast("timestamp")
        )


def main():
    print("=" * 50)
    print(" 코인 스트리밍 Spark Job 시작")
    print("=" * 50)

    # 1. Spark 세션 생성
    print("[1] Spark 세션 생성 중...")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")  # 불필요한 로그 줄이기
    print("[2] Spark 세션 생성 완료\n")

    # 2. Kafka에서 읽기
    print("[3] Kafka 연결 중...")
    raw_df = read_from_kafka(spark)
    print("[4] Kafka 연결 완료\n")

    # 3. JSON 파싱
    parsed_df = parse_kafka_data(raw_df)

    # 4. 1분봉 집계
    ohlcv_df = aggregate_ohlcv(parsed_df)

    # 5. 이상탐지
    #result_df = detect_anomaly(ohlcv_df)

    # 6. PostgreSQL upsert 저장
    def write_to_postgres(batch_df, batch_id):
        """
        1분마다 완성된 배치를 PostgreSQL에 upsert하는 함수.
        (code + window_start) 조합이 이미 있으면 UPDATE, 없으면 INSERT.
        """
        rows = batch_df.select(
            col("code"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("open"),
            col("high"),
            col("low"),
            col("close"),
            col("volume"),
            col("trade_count"),
        ).collect()  # Python 리스트로 변환

        if not rows:
            return

        conn = psycopg2.connect(**PG_CONN)
        cur = conn.cursor()

        upsert_sql = """
            INSERT INTO ohlcv
                (code, window_start, window_end, open, high, low, close, volume, trade_count)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (code, window_start)
            DO UPDATE SET
                window_end   = EXCLUDED.window_end,
                open         = EXCLUDED.open,
                high         = EXCLUDED.high,
                low          = EXCLUDED.low,
                close        = EXCLUDED.close,
                volume       = EXCLUDED.volume,
                trade_count  = EXCLUDED.trade_count
        """

        cur.executemany(upsert_sql, [
            (r.code, r.window_start, r.window_end,
             r.open, r.high, r.low, r.close, r.volume, r.trade_count)
            for r in rows
        ])

        conn.commit()
        cur.close()
        conn.close()

    query = ohlcv_df \
        .writeStream \
        .foreachBatch(write_to_postgres) \
        .outputMode("update") \
        .start()

    print("[5] 스트리밍 시작 — 1분마다 PostgreSQL에 저장\n")

    query.awaitTermination()


if __name__ == "__main__":
    main()
