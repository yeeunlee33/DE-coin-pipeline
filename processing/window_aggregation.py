from pyspark.sql import DataFrame
from pyspark.sql.functions import window, avg, max, min, first, last, sum, count


def aggregate_ohlcv(df: DataFrame) -> DataFrame:
    """
    틱 데이터 → 1분봉 OHLCV 집계

    OHLCV = Open(시가) / High(고가) / Low(저가) / Close(종가) / Volume(거래량)
    """
    return df \
        .withWatermark("event_time", "10 seconds") \
            .groupBy(
            window("event_time", "1 minute"),          # 1분 윈도우
            "code"                                     # 코인별로
        ).agg(
            first("trade_price").alias("open"),        # 시가 (첫 번째 체결가)
            max("trade_price").alias("high"),           # 고가
            min("trade_price").alias("low"),            # 저가
            last("trade_price").alias("close"),         # 종가 (마지막 체결가)
            sum("trade_volume").alias("volume"),        # 거래량 합계
            count("trade_price").alias("trade_count"),  # 체결 횟수
        )
