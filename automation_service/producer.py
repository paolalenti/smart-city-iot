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


def send_command(device_id: str, command: str):
    """Отправляет событие в топик commands"""
    payload = {
        'device_id': device_id,
        'command': command
    }

    producer.produce(
        topic="commands",
        value=json.dumps(payload).encode('utf-8'),
        callback=delivery_report
    )

    producer.poll(0)
    producer.flush()
