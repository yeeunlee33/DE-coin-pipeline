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


def is_anomaly(change_rate: float, threshold: float = 3.0) -> bool:
    """변동률 절댓값이 threshold 이상이면 이상 신호"""
    return abs(change_rate) >= threshold
