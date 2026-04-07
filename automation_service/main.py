import os
import json
from confluent_kafka import Consumer
from producer import send_notification

KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
print(f"DEBUG: Connecting to Kafka at {KAFKA_SERVER}")

conf = {
    'bootstrap.servers': KAFKA_SERVER,
    'group.id': 'automation_service',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(conf)
consumer.subscribe(['alerts'])

while True:
    msg = consumer.poll(0)
    if msg is None: continue
    if msg.error():
        print(f"Ошибка получения сообщения: {msg.error()}")
        continue

    payload = json.loads(msg.value().decode("utf-8"))

    alert_type = payload['alert_type']

    print(f"Получена тревога: {alert_type}")

    # Заглушка, пишет логи в консоль
    # Можно заменить на отправку уведомлений
    match alert_type:
        case "temperature-high":
            send_notification("INFO", "Включаю полив")
        case "temperature-low":
            send_notification("INFO", "Включаю отопление")
        case "humidity-high":
            send_notification("INFO", "Включаю проветривание")
        case "humidity-low":
            send_notification("INFO", "Включаю полив")

consumer.close()
