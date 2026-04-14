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
    'temperature': (20, 25, 35, 40),
    'humidity': (40, 45, 55, 60)
}


try:
    while True:
        msg = consumer.poll()
        if msg is None: continue
        if msg.error():
            print(f"Ошибка получения сообщения: {msg.error()}")
            continue

        payload = json.loads(msg.value().decode("utf-8"))

        device_serial = payload.get('serial_code', None)
        metric = payload.get('metric', None)
        val = payload.get('value', None)
        if device_serial is None or metric is None or val is None: continue

        extreme_min, norm_min, norm_max, extreme_max = sensor_reading_limits[metric]
        if val >= extreme_max:
            send_alert(device_serial, metric, high=True, extreme=True)
        elif val <= extreme_min:
            send_alert(device_serial, metric, high=False, extreme=True)
        elif val > norm_max:
            send_alert(device_serial, metric, high=True, extreme=False)
        elif val < norm_min:
            send_alert(device_serial, metric, high=False, extreme=False)
finally:
    consumer.close()
