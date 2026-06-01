from pyspark.sql import DataFrame
from pyspark.sql.functions import lag, abs as spark_abs, col
from pyspark.sql.window import Window


def detect_anomaly(df: DataFrame, threshold: float = 3.0) -> DataFrame:
    """
    직전 분봉 대비 가격 변동률이 threshold(%) 이상이면 이상 신호 발생

    threshold 기본값 = 3.0 (3% 이상 급등/급락)
    """

    # 코인별로 시간 순서대로 정렬
    window_spec = Window.partitionBy("code").orderBy("window")

    return df \
        .withColumn(
            "prev_close",
            lag("close", 1).over(window_spec)       # 직전 분봉 종가
        ) \
        .withColumn(
            "change_rate",
            (col("close") - col("prev_close"))
            / col("prev_close") * 100               # 변동률 (%)
        ) \
        .withColumn(
            "is_anomaly",
            spark_abs(col("change_rate")) >= threshold  # 3% 이상이면 True
        )
