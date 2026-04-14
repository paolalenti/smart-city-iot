import os
import json

from confluent_kafka import Consumer

import models

from main import invalidate_device_cache, set_device_cache
from database import SessionLocal


def _handle_unit_created(db, data: dict):
    existing = db.query(models.Unit).filter(models.Unit.uid == data["uid"]).first()
    if existing:
        existing.name = data["name"]
        existing.location = data.get("location")
        existing.is_active = data.get("is_active", True)
    else:
        db.add(models.Unit(
            id=data["id"],
            uid=data["uid"],
            name=data["name"],
            location=data.get("location"),
            is_active=data.get("is_active", True),
        ))
    db.commit()
    print(f"[mirror] Unit uid={data['uid']} синхронизирован")


def _handle_unit_updated(db, data: dict):
    existing = db.query(models.Unit).filter(
        models.Unit.uid == data["uid"]
    ).first()

    if existing is None:
        print(f"[mirror] Unit uid={data['uid']} не найден для обновления")
        return

    existing.name = data["name"]
    existing.type = data["type"]
    existing.location = data.get("location")
    existing.is_active = data.get("is_active", True)

    db.commit()
    db.refresh(existing)

    print(f"[mirror] Unit uid={data['uid']} обновлён")


def _handle_unit_deleted(db, data: dict):
    unit = db.query(models.Unit).filter(models.Unit.id == data["unit_id"]).first()
    if unit:
        db.delete(unit)
        db.commit()
        print(f"[mirror] Unit id={data['unit_id']} удалён из зеркала")


def _handle_device_created(db, data: dict):
    existing = db.query(models.Device).filter(
        models.Device.serial_code == data["serial_code"]
    ).first()

    if existing:
        existing.name = data["name"]
        existing.type = data["type"]
        existing.location = data.get("location")
        existing.is_active = data.get("is_active", True)
        existing.unit_id = data.get("unit_id")
        db.commit()

        set_device_cache(existing)
    else:
        device = models.Device(
            id=data["id"],
            serial_code=data["serial_code"],
            name=data["name"],
            type=data["type"],
            location=data.get("location"),
            is_active=data.get("is_active", True),
            unit_id=data.get("unit_id"),
        )
        db.add(device)
        db.commit()
        db.refresh(device)
        set_device_cache(device)

    print(f"[mirror] Device serial={data['serial_code']} синхронизирован")


def _handle_device_updated(db, data: dict):
    existing = db.query(models.Device).filter(
        models.Device.serial_code == data["serial_code"]
    ).first()

    if existing is None:
        print(f"[mirror] Device serial={data['serial_code']} не найден для обновления")
        return

    existing.name = data["name"]
    existing.type = data["type"]
    existing.location = data.get("location")
    existing.is_active = data.get("is_active", True)
    existing.unit_id = data.get("unit_id")
    db.commit()
    db.refresh(existing)

    set_device_cache(existing)
    print(f"[mirror] Device serial={data['serial_code']} обновлён")


def _handle_device_deleted(db, data: dict):
    device = db.query(models.Device).filter(models.Device.id == data["device_id"]).first()
    if device:
        invalidate_device_cache(device.serial_code)
        db.delete(device)
        db.commit()
        print(f"[mirror] Device id={data['device_id']} удалён из зеркала")


_HANDLERS = {
    "unit_created": _handle_unit_created,
    "unit_updated": _handle_unit_updated,
    "unit_deleted": _handle_unit_deleted,
    "created": _handle_device_created,
    "updated": _handle_device_updated,
    "deleted": _handle_device_deleted,
}


def listen():
    KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    print(f"DEBUG: Connecting to Kafka at {KAFKA_SERVER}")

    conf = {
        "bootstrap.servers": KAFKA_SERVER,
        "group.id": "telemetry_service_mirror",
        "auto.offset.reset": "earliest",
    }
    consumer = Consumer(conf)
    consumer.subscribe(["device_events"])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Ошибка получения сообщения: {msg.error()}")
                continue

            payload = json.loads(msg.value().decode("utf-8"))
            event = payload.get("event")
            data = payload.get("data", {})

            handler = _HANDLERS.get(event)
            if handler is None:
                print(f"[mirror] Неизвестный тип события: {event}")
                continue

            db = SessionLocal()
            try:
                handler(db, data)
            except Exception as e:
                print(f"[mirror] Ошибка обработки события '{event}': {e}")
                db.rollback()
            finally:
                db.close()
    finally:
        consumer.close()
