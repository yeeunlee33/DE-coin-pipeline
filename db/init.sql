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
