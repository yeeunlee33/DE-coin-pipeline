CREATE TABLE IF NOT EXISTS ohlcv (
    id           SERIAL PRIMARY KEY,
    code         VARCHAR(20)      NOT NULL,        -- 코인 종목 (예: KRW-BTC)
    window_start TIMESTAMP        NOT NULL,        -- 1분봉 시작 시각
    window_end   TIMESTAMP        NOT NULL,        -- 1분봉 종료 시각
    open         DOUBLE PRECISION NOT NULL,        -- 시가
    high         DOUBLE PRECISION NOT NULL,        -- 고가
    low          DOUBLE PRECISION NOT NULL,        -- 저가
    close        DOUBLE PRECISION NOT NULL,        -- 종가
    volume       DOUBLE PRECISION NOT NULL,        -- 거래량
    trade_count  BIGINT           NOT NULL,        -- 체결 횟수
    created_at   TIMESTAMP        DEFAULT NOW(),   -- 저장 시각

    -- code + window_start 조합이 같으면 중복으로 판단
    UNIQUE (code, window_start)
);

-- 코인별 이상탐지 임계값 (Airflow가 매일 재계산해서 저장)
CREATE TABLE IF NOT EXISTS anomaly_threshold (
    id             SERIAL PRIMARY KEY,
    code           VARCHAR(20)      NOT NULL,   -- 코인 종목
    threshold      DOUBLE PRECISION NOT NULL,   -- 계산된 임계값 (%)
    avg_change     DOUBLE PRECISION NOT NULL,   -- 최근 7일 변동률 평균
    std_change     DOUBLE PRECISION NOT NULL,   -- 최근 7일 변동률 표준편차
    calculated_at  TIMESTAMP        DEFAULT NOW(),

    UNIQUE (code)  -- 코인당 최신 임계값 하나만 유지
);

CREATE TABLE IF NOT EXISTS anomaly_log (
    id           SERIAL PRIMARY KEY,
    code         VARCHAR(20)      NOT NULL,
    window_start TIMESTAMP        NOT NULL,
    window_end   TIMESTAMP        NOT NULL,
    open         DOUBLE PRECISION NOT NULL,
    high         DOUBLE PRECISION NOT NULL,
    low          DOUBLE PRECISION NOT NULL,
    close        DOUBLE PRECISION NOT NULL,
    volume       DOUBLE PRECISION NOT NULL,
    trade_count  BIGINT           NOT NULL,
    prev_close   DOUBLE PRECISION NOT NULL,  -- 직전 분봉 종가
    change_rate  DOUBLE PRECISION NOT NULL,  -- 변동률 (%)
    detected_at  TIMESTAMP        DEFAULT NOW(),

    UNIQUE (code, window_start)  -- 분봉당 이상 탐지 기록 하나만 유지
);