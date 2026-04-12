import os
import json

from confluent_kafka import Consumer
from influxdb_client import Point
from influxdb_client.client.exceptions import InfluxDBError
from influx_client import write_api, INFLUX_BUCKET, INFLUX_ORG


def listen():
    KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    print(f"DEBUG: Connecting to Kafka at {KAFKA_SERVER}")

    conf = {
        "bootstrap.servers": KAFKA_SERVER,
        "group.id": "hist_service",
        "auto.offset.reset": "earliest",
    }
    consumer = Consumer(conf)
    consumer.subscribe(["telemetry"])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Ошибка получения сообщения: {msg.error()}")
                continue

            payload = json.loads(msg.value().decode("utf-8"))
            print(f"[hist] Получен показатель: {payload}")

            try:
                point = (
                    Point(payload["metric"])
                    .tag("device_id",   str(payload["device_id"]))
                    .tag("serial_code", str(payload["serial_code"]))
                    .tag("device_type", payload.get("device_type", "unknown"))
                    .tag("location",    payload.get("location") or "unknown")
                    .field("value",     float(payload["value"]))
                    .time(payload["timestamp"])
                )
                if payload.get("unit"):
                    point = point.tag("unit", payload["unit"])

                write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
                print(f"[hist] Записано в InfluxDB: device_id={payload['device_id']} "
                      f"metric={payload['metric']} value={payload['value']}")

            except InfluxDBError as e:
                print(f"[hist] Ошибка записи в InfluxDB: {e}")
            except (KeyError, ValueError) as e:
                print(f"[hist] Некорректный payload: {e} | {payload}")

    finally:
        consumer.close()
