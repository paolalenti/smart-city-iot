import os
import json
import requests
from threading import Thread
from confluent_kafka import Consumer
from database import SessionLocal
import models
from producer import send_notification


def _send_command(endpoint, name, command):
    try:
        response = requests.post(endpoint, json={'command': command}, timeout=5)
        if response.status_code == 200: return
    finally:
        send_notification('ERROR', f"Не удалось отправить команду устройству {name}")


def listen():
    KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    print(f"DEBUG: Connecting to Kafka at {KAFKA_SERVER}")

    conf = {
        'bootstrap.servers': KAFKA_SERVER,
        'group.id': 'automation_service',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(conf)
    consumer.subscribe(['commands'])

    try:
        while True:
            msg = consumer.poll()
            if msg is None: continue
            if msg.error():
                print(f"Ошибка получения сообщения: {msg.error()}")
                continue

            payload = json.loads(msg.value().decode("utf-8"))
            print(f"Получена команда: {payload}")

            device_serial = payload.get('device_serial', None)
            command = payload.get('command', None)
            if device_serial is None or command is None: continue

            db = SessionLocal()
            db_device = db.query(models.Device).filter(models.Device.serial_code == device_serial).first()
            if db_device is not None:
                Thread(target=_send_command, args=(db_device.api_endpoint, db_device.name, command), daemon=True).start()
            else:
                send_notification('WARN', f"Не удалось отправить команду устройству UNKNOWN")
            db.close()
    finally:
        consumer.close()
