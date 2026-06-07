"""
이상탐지 임계값 재계산 DAG

매일 자정에 실행:
  Task1. 코인별 최근 7일 분봉 변동률 계산
  Task2. 평균 + 3*표준편차로 임계값 계산
  Task3. anomaly_threshold 테이블에 upsert
"""

from datetime import datetime, timedelta

import os

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

# ── PostgreSQL 연결 설정 ───────────────────────────────────────
# Airflow 컨테이너는 도커 내부에서 실행되므로 host는 컨테이너 이름 사용
PG_CONN = {
    "host":     os.getenv("POSTGRES_HOST", "postgres"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname":   os.getenv("POSTGRES_DB", "coindb"),
    "user":     os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", ""),
}

COINS = [
    "KRW-BTC",
    "KRW-ETH",
    "KRW-XRP",
    "KRW-SOL",
    "KRW-DOGE",
    "KRW-ADA",
]


def calculate_and_update_threshold():
    """
    코인별 최근 7일 분봉 데이터로 변동률 통계 계산 후 임계값 upsert

    임계값 = 평균 변동률 + 3 * 표준편차
    → 정규분포 기준 99.7% 범위 밖이면 이상으로 판단
    """
    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()

    # 최근 7일 분봉에서 직전 분봉 대비 변동률 계산
    # LAG()를 배치(정적 DataFrame)에서 사용 → 여기선 문제없음
    stats_sql = """
        SELECT
            code,
            AVG(ABS(change_rate))  AS avg_change,
            STDDEV(ABS(change_rate)) AS std_change
        FROM (
            SELECT
                code,
                close,
                LAG(close) OVER (PARTITION BY code ORDER BY window_start) AS prev_close,
                (close - LAG(close) OVER (PARTITION BY code ORDER BY window_start))
                / NULLIF(LAG(close) OVER (PARTITION BY code ORDER BY window_start), 0) * 100
                AS change_rate
            FROM ohlcv
            WHERE window_start >= NOW() - INTERVAL '7 days'
        ) sub
        WHERE change_rate IS NOT NULL
        GROUP BY code
    """

    upsert_sql = """
        INSERT INTO anomaly_threshold (code, threshold, avg_change, std_change)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (code)
        DO UPDATE SET
            threshold     = EXCLUDED.threshold,
            avg_change    = EXCLUDED.avg_change,
            std_change    = EXCLUDED.std_change,
            calculated_at = NOW()
    """

    cur.execute(stats_sql)
    rows = cur.fetchall()

    if not rows:
        print("[WARNING] 최근 7일 데이터 없음 — 임계값 업데이트 스킵")
        cur.close()
        conn.close()
        return

    for code, avg_change, std_change in rows:
        # 데이터가 부족해서 표준편차가 None이면 기본값 3.0 사용
        if avg_change is None or std_change is None:
            threshold = 3.0
        else:
            threshold = avg_change + 3 * std_change

        print(f"[{code}] 평균: {avg_change:.2f}% | 표준편차: {std_change:.2f}% | 임계값: {threshold:.2f}%")
        cur.execute(upsert_sql, (code, threshold, avg_change, std_change))

    conn.commit()
    cur.close()
    conn.close()
    print("임계값 업데이트 완료")


# ── DAG 정의 ──────────────────────────────────────────────────
with DAG(
    dag_id="threshold_update",
    description="코인별 이상탐지 임계값 매일 재계산",
    schedule="0 0 * * *",        # 매일 자정 실행 (cron 표현식)
    start_date=datetime(2026, 1, 1),
    catchup=False,                # 과거 누락된 실행 건너뜀
    default_args={
        "retries": 1,             # 실패 시 1회 재시도
        "retry_delay": timedelta(minutes=5),
    },
) as dag:

    update_threshold = PythonOperator(
        task_id="calculate_and_update_threshold",
        python_callable=calculate_and_update_threshold,
    )
