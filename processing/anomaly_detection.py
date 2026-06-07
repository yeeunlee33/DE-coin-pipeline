DEFAULT_THRESHOLD = 3.0  # Airflow가 아직 임계값 계산 안 했을 때 기본값


def calculate_change_rate(current_close: float, prev_close: float) -> float:
    """
    직전 분봉 대비 현재 분봉의 가격 변동률(%) 계산

    이전 코드의 문제:
    - Spark Window + lag()는 정적 DataFrame 전용
    - Structured Streaming에서는 "이전 행"이 메모리에 없어서 동작 불가
    → foreachBatch 안에서 PostgreSQL에 직전 분봉을 직접 조회해서 해결
    """
    if prev_close is None or prev_close == 0:
        return 0.0
    return (current_close - prev_close) / prev_close * 100


def get_threshold(cur, code: str) -> float:
    """
    DB에서 코인별 임계값 조회
    Airflow가 아직 실행 전이거나 데이터 부족으로 임계값이 없으면 기본값 사용
    """
    cur.execute(
        "SELECT threshold FROM anomaly_threshold WHERE code = %s",
        (code,)
    )
    result = cur.fetchone()
    return result[0] if result else DEFAULT_THRESHOLD


def is_anomaly(change_rate: float, threshold: float) -> bool:
    """변동률 절댓값이 threshold 이상이면 이상 신호"""
    return abs(change_rate) >= threshold
