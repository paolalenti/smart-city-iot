import os
import json
from confluent_kafka import Producer

KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
print(f"DEBUG: Connecting to Kafka at {KAFKA_SERVER}")

conf = {'bootstrap.servers': KAFKA_SERVER}
producer = Producer(conf)


def delivery_report(err, msg):
    """ Отчет о доставке сообщения (callback) """
    if err is not None:
        print(f"Ошибка доставки сообщения: {err}")
    else:
        print(f"Сообщение доставлено в {msg.topic()} [{msg.partition()}]")


def send_alert(device_id: str, reading: str, high: bool, extreme: bool):
    """Отправляет событие в топик alerts"""
    payload = {
        'device_id': device_id,
        'reading': reading,
        'state': 'high' if high else 'low',
        'severity': 'high' if extreme else 'low'
    }

    producer.produce(
        topic="alerts",
        value=json.dumps(payload).encode('utf-8'),
        callback=delivery_report
    )

    producer.poll(0)
    producer.flush()
