from pyspark.sql.types import StructType, StringType, DoubleType, LongType

# 업비트 체결 데이터 스키마
# parse_trade()에서 추출한 필드와 동일하게 맞춰야 함
TRADE_SCHEMA = StructType() \
    .add("code",         StringType()) \
        .add("trade_price",  DoubleType()) \
            .add("trade_volume", DoubleType()) \
                .add("timestamp",    LongType())   \
                    .add("ask_bid",      StringType())     # 매수(ASK) / 매도(BID)
