import os
import json
from confluent_kafka import Consumer
from producer import send_notification, send_command

KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
print(f"DEBUG: Connecting to Kafka at {KAFKA_SERVER}")

conf = {
    'bootstrap.servers': KAFKA_SERVER,
    'group.id': 'automation_service',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(conf)
consumer.subscribe(['alerts'])


command_map = {
    ('temperature', 'high'): 'activate watering',
    ('temperature', 'low'): 'activate heating',
    ('humidity', 'high'): 'activate ventilation',
    ('humidity', 'low'): 'activate watering'
}

notification_map = {
    ('temperature', 'high'): "Критично высокая температура, примите меры!",
    ('temperature', 'low'): "Критично низкая температура, примите меры!",
    ('humidity', 'high'): "Критично высокая влажность, примите меры!",
    ('humidity', 'low'): "Критично низкая влажность, примите меры!"
}


try:
    while True:
        msg = consumer.poll()
        if msg is None: continue
        if msg.error():
            print(f"Ошибка получения сообщения: {msg.error()}")
            continue

        payload = json.loads(msg.value().decode("utf-8"))
        print(f"Получена тревога: {payload}")

        device_id = payload.get('device_id', None)
        reading = payload.get('reading', None)
        state = payload.get('state', None)
        severity = payload.get('severity', None)
        if device_id is None or reading is None or state is None or severity is None: continue

        if severity == 'low':
            command = command_map.get((reading, state), None)
            if command is None: continue
            send_command(device_id, command)
        elif severity == 'high':
            notification = notification_map.get((reading, state), None)
            if notification is None: continue
            send_command(device_id, 'activate alarm')
            send_notification('WARN', notification)
finally:
    consumer.close()
