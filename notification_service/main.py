import os
import json
from confluent_kafka import Consumer
from bot import send_notification

KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
print(f"DEBUG: Connecting to Kafka at {KAFKA_SERVER}")

conf = {
    'bootstrap.servers': KAFKA_SERVER,
    'group.id': 'alert_engine',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(conf)
consumer.subscribe(['notifications'])


try:
    while True:
        msg = consumer.poll()
        if msg is None: continue
        if msg.error():
            print(f"Ошибка получения сообщения: {msg.error()}")
            continue

        payload = json.loads(msg.value().decode("utf-8"))

        notification_type = payload.get('notification_type', None)
        message = payload.get('message', None)

        send_notification(notification_type, message)
finally:
    consumer.close()
