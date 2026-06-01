from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

for i in range(10):
    data = {
        "coin": "BTC",
        "price": 100000000 + i
    }

    producer.send("upbit-trades", value=data)

    print(f"Sent: {data}")

    time.sleep(1)

producer.flush()