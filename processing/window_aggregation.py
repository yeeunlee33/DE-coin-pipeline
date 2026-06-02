from pyspark.sql import DataFrame
from pyspark.sql.functions import window, max, min, min_by, max_by, sum, count


def aggregate_ohlcv(df: DataFrame) -> DataFrame:
    """
    틱 데이터 → 1분봉 OHLCV 집계

    OHLCV = Open(시가) / High(고가) / Low(저가) / Close(종가) / Volume(거래량)

    open  : 1분 윈도우 안에서 event_time이 가장 빠른 체결가 (min_by)
    close : 1분 윈도우 안에서 event_time이 가장 늦은 체결가 (max_by)
    → first()/last()는 Spark 내부 처리 순서 기준이라 이벤트 시각과 무관함
    """
    return df \
        .withWatermark("event_time", "10 seconds") \
        .groupBy(
            window("event_time", "1 minute"),           # 1분 윈도우
            "code"                                      # 코인별로
        ).agg(
            min_by("trade_price", "event_time").alias("open"),   # 시가: 가장 빠른 체결가
            max("trade_price").alias("high"),                     # 고가
            min("trade_price").alias("low"),                      # 저가
            max_by("trade_price", "event_time").alias("close"),  # 종가: 가장 늦은 체결가
            sum("trade_volume").alias("volume"),                  # 거래량 합계
            count("trade_price").alias("trade_count"),            # 체결 횟수
        )
