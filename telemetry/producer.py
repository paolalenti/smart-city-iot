import os
import json
from confluent_kafka import Producer

KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
print(f"DEBUG: Connecting to Kafka at {KAFKA_SERVER}")

_producer = Producer({'bootstrap.servers': KAFKA_SERVER})


def _delivery_report(err, msg):
    if err is not None:
        print(f"Ошибка доставки сообщения: {err}")
    else:
        print(f"Сообщение доставлено в {msg.topic()} [{msg.partition()}]")


def send_telemetry_event(payload: dict):
    """Публикует показатель в топик telemetry."""
    _producer.produce(
        topic="telemetry",
        value=json.dumps(payload).encode("utf-8"),
        callback=_delivery_report,
    )
    _producer.poll(0)
    _producer.flush()


