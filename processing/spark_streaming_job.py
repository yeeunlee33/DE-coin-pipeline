import logging
import os

import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, from_unixtime

from anomaly_detection import calculate_change_rate, get_threshold, is_anomaly
from schemas import TRADE_SCHEMA
from window_aggregation import aggregate_ohlcv

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# 텔레그램 봇 설정 (환경변수로 관리 — 코드에 직접 넣지 말 것)
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

TELEGRAM_BOT_TOKEN = os.getenv("telegram_token", "")
TELEGRAM_CHAT_ID   = os.getenv("telegram_cht_id", "")


def send_telegram_alert(code: str, close: float, change_rate: float) -> None:
    """
    이상 탐지 시 텔레그램 메시지 전송

    토큰/chat_id가 없으면 조용히 스킵 (개발 환경 배려)
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.warning("[텔레그램] 토큰 또는 chat_id 미설정 — 전송 스킵")
        return

    import urllib.request  # 외부 라이브러리 없이 표준 라이브러리만 사용
    import urllib.parse

    direction = "🚀 급등" if change_rate > 0 else "📉 급락"
    message = (
        f"[이상 탐지] {code}\n"
        f"{direction} {change_rate:+.2f}%\n"
        f"현재가: {close:,.0f}원"
    )

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    data = urllib.parse.urlencode({
        "chat_id": TELEGRAM_CHAT_ID,
        "text":    message,
    }).encode()

    req = urllib.request.Request(url, data=data)
    urllib.request.urlopen(req, timeout=5)

# ── 환경 설정 ──────────────────────────────────────────────
os.environ["HADOOP_HOME"] = "C:/hadoop"

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:29092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "upbit-trades")
CHECKPOINT_DIR          = os.getenv("CHECKPOINT_DIR", "C:/tmp/spark-checkpoint")

# ── PostgreSQL 설정 ────────────────────────────────────────
PG_CONN = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname":   os.getenv("POSTGRES_DB", "coindb"),
    "user":     os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", ""),
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

    # 6. PostgreSQL upsert + 이상 탐지 + 텔레그램 알람
    def write_to_postgres(batch_df, batch_id):
        """
        Spark가 1분봉을 완성할 때마다 호출되는 핸들러.
        이 배치의 분봉을 저장한 직후, 같은 DB에서 직전 분봉을 조회해 이상 탐지.

          1) ohlcv upsert → commit : 이 배치 분봉 저장 확정
          2) 직전 분봉 조회 → 이상 탐지 → anomaly_log 저장
          3) 텔레그램 전송 : 실패해도 저장에 영향 없음
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
        ).collect()

        if not rows:
            return

        conn = psycopg2.connect(**PG_CONN)
        cur = conn.cursor()

        # ── 1) ohlcv upsert ───────────────────────────────────────────────
        upsert_sql = """
            INSERT INTO ohlcv
                (code, window_start, window_end, open, high, low, close, volume, trade_count)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (code, window_start)
            DO UPDATE SET
                window_end  = EXCLUDED.window_end,
                open        = EXCLUDED.open,
                high        = EXCLUDED.high,
                low         = EXCLUDED.low,
                close       = EXCLUDED.close,
                volume      = EXCLUDED.volume,
                trade_count = EXCLUDED.trade_count
        """
        cur.executemany(upsert_sql, [
            (r.code, r.window_start, r.window_end,
             r.open, r.high, r.low, r.close, r.volume, r.trade_count)
            for r in rows
        ])
        conn.commit()  # 저장은 여기서 무조건 확정

        # ── 2) 이상 탐지 + anomaly_log 저장 ──────────────────────────────
        anomaly_sql = """
            INSERT INTO anomaly_log
                (code, window_start, window_end, open, high, low, close,
                 volume, trade_count, prev_close, change_rate)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        # 직전 분봉 close를 DB에서 조회하는 쿼리
        # window_start보다 이전 분봉 중 가장 최신 것을 가져옴
        prev_close_sql = """
            SELECT close FROM ohlcv
            WHERE code = %s AND window_start < %s
            ORDER BY window_start DESC
            LIMIT 1
        """

        anomalies = []  # 텔레그램 전송용 버퍼

        for r in rows:
            cur.execute(prev_close_sql, (r.code, r.window_start))
            result = cur.fetchone()
            prev_close = result[0] if result else None  # 첫 번째 분봉이면 None

            change_rate = calculate_change_rate(r.close, prev_close)
            threshold   = get_threshold(cur, r.code)  # 코인별 임계값 조회

            if is_anomaly(change_rate, threshold):
                cur.execute(anomaly_sql, (
                    r.code, r.window_start, r.window_end,
                    r.open, r.high, r.low, r.close,
                    r.volume, r.trade_count,
                    prev_close, change_rate,
                ))
                anomalies.append((r.code, r.close, change_rate))

        conn.commit()
        cur.close()
        conn.close()

        # ── 3) 텔레그램 알람 ─────────────────────────────────────────────
        # DB 커밋 완료 후에 전송 → 텔레그램 실패가 저장에 영향 없음
        for code, close, change_rate in anomalies:
            try:
                send_telegram_alert(code, close, change_rate)
            except Exception as e:
                # 알람 실패는 로그만 남기고 계속 진행
                logging.error(f"[텔레그램 전송 실패] {code}: {e}")

    query = ohlcv_df \
        .writeStream \
        .foreachBatch(write_to_postgres) \
        .outputMode("update") \
        .start()

    print("[5] 스트리밍 시작 — 1분마다 PostgreSQL에 저장\n")

    query.awaitTermination()


if __name__ == "__main__":
    main()
