import asyncio
import json

import websockets
from kafka import KafkaProducer

from kafka_config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    UPBIT_COINS,
    UPBIT_WS_URL,
)


def create_producer() -> KafkaProducer:
    """Kafka Producer 생성"""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def build_subscribe_msg() -> list:
    """업비트 WebSocket 구독 메시지 생성"""
    return [
        {"ticket": "coin-pipeline"},
        {
            "type": "trade",       # 실시간 체결 데이터
            "codes": UPBIT_COINS,
        },
    ]


def parse_trade(data: dict) -> dict:
    """업비트 원본 데이터에서 필요한 필드만 추출"""
    return {
        "code":         data.get("code"),           # 코인 종류 (KRW-BTC)
        "trade_price":  data.get("trade_price"),    # 체결 가격
        "trade_volume": data.get("trade_volume"),   # 체결량
        "timestamp":    data.get("timestamp"),      # 체결 시각 (ms)
        "ask_bid":      data.get("ask_bid"),         # 매수(ASK) / 매도(BID)
    }


async def stream_to_kafka(producer: KafkaProducer) -> None:
    """업비트 WebSocket → Kafka 스트리밍"""
    print(f"[INFO] 연결 시도: {UPBIT_WS_URL}")
    print(f"[INFO] 구독 코인: {UPBIT_COINS}")

    async with websockets.connect(UPBIT_WS_URL) as ws:
        await ws.send(json.dumps(build_subscribe_msg()))
        print("[INFO] 업비트 연결 완료 — 데이터 수신 중...\n")

        while True:
            raw  = await ws.recv()
            data = json.loads(raw)

            trade = parse_trade(data)

            # 코인 종류를 파티션 키로 설정 (같은 코인 → 같은 파티션 → 순서 보장)
            key = trade["code"].encode("utf-8")

            producer.send(KAFKA_TOPIC, key=key, value=trade)

            print(
                f"[전송] {trade['code']:<10} | "
                f"가격: {trade['trade_price']:>15,.0f}원 | "
                f"체결량: {trade['trade_volume']:.6f} | "
                f"{trade['ask_bid']}"
            )


def main():
    print("=" * 50)
    print(" 업비트 실시간 Producer 시작")
    print("=" * 50)

    print("[1] Kafka Producer 생성 중...")
    producer = create_producer()
    print(f"[2] Kafka 연결 완료: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"[3] 토픽: {KAFKA_TOPIC}\n")

    try:
        asyncio.run(stream_to_kafka(producer))
    except KeyboardInterrupt:
        print("\n[INFO] 종료 중...")
    finally:
        producer.flush()   # 버퍼에 남은 메시지 전송
        producer.close()
        print("[INFO] Producer 종료 완료")


if __name__ == "__main__":
    main()
