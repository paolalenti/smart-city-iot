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


def send_device_event(event_type: str, device_data: dict):
    """
    Отправляет событие в топик device_events
    event_type: 'created' или 'deleted'
    """
    payload = {
        "event": event_type,
        "data": device_data
    }

    producer.produce(
        topic="device_events",
        value=json.dumps(payload).encode('utf-8'),
        callback=delivery_report
    )

    producer.poll(0)
    producer.flush()


def send_notification(notification_type: str, message: str):
    """Отправляет событие в топик notifications"""
    payload = {
        'notification_type': notification_type,
        'message': message
    }

    producer.produce(
        topic="notifications",
        value=json.dumps(payload).encode('utf-8'),
        callback=delivery_report
    )

    producer.poll(0)
    producer.flush()
