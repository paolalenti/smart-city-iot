import os
import json
from confluent_kafka import Consumer
from producer import send_alert

KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
print(f"DEBUG: Connecting to Kafka at {KAFKA_SERVER}")

conf = {
    'bootstrap.servers': KAFKA_SERVER,
    'group.id': 'alert_engine',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(conf)
consumer.subscribe(['telemetry'])


sensor_reading_limits = {
    'temperature': (20, 50),
    'humidity': (40, 60)
}


try:
    while True:
        msg = consumer.poll()
        if msg is None: continue
        if msg.error():
            print(f"Ошибка получения сообщения: {msg.error()}")
            continue

        payload = json.loads(msg.value().decode("utf-8"))

        for reading, limits in sensor_reading_limits.items():
            min_v, max_v = limits
            val = payload.get(reading, None)
            if val is None: continue

            if val > max_v: send_alert(reading, True)
            if val < min_v: send_alert(reading, False)
finally:
    consumer.close()
