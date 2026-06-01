import os

# ── Kafka 연결 설정 ────────────────────────────────────────
# 로컬 실행 → 127.0.0.1:29092 (기본값)
# Docker 실행 → docker-compose.yml에서 kafka:9092 주입
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:29092")

# ── 토픽 설정 ──────────────────────────────────────────────
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "upbit-trades")

# ── 업비트 구독 코인 목록 ───────────────────────────────────
UPBIT_COINS = [
    "KRW-BTC",   # 비트코인
    "KRW-ETH",   # 이더리움
    "KRW-XRP",   # 리플
]

# ── 업비트 WebSocket 주소 ───────────────────────────────────
UPBIT_WS_URL = "wss://api.upbit.com/websocket/v1"
