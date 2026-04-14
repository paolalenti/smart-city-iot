import os
import json
import redis

from datetime import datetime, timezone
from fastapi import FastAPI, Depends, HTTPException, Request
from sqlalchemy.orm import Session

import models
import schemas

from database import engine, get_db
from producer import send_telemetry_event

models.Base.metadata.create_all(bind=engine)

app = FastAPI(title="IoT Telemetry Service", root_path="/telemetry")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=1, decode_responses=True)

RATE_LIMIT = 100
RATE_LIMIT_WINDOW = 60
DEVICE_CACHE_TTL = 300


def check_rate_limit(request: Request):
    ip = request.client.host
    key = f"rate_limit:telemetry_service:{ip}"
    current = redis_client.incr(key)
    if current == 1:
        redis_client.expire(key, RATE_LIMIT_WINDOW)
    if current > RATE_LIMIT:
        raise HTTPException(status_code=429, detail="Too Many Requests")


def get_device_from_cache(serial_code: int):
    """Достаёт данные устройства из Redis. Возвращает dict или None."""
    cached = redis_client.get(f"device:{serial_code}")
    if cached:
        return json.loads(cached)
    return None


def set_device_cache(device: models.Device):
    """Кэширует устройство в Redis на DEVICE_CACHE_TTL секунд."""
    data = {
        "id": device.id,
        "serial_code": device.serial_code,
        "name": device.name,
        "type": device.type,
        "location": device.location,
        "is_active": device.is_active,
    }
    redis_client.setex(f"device:{device.serial_code}", DEVICE_CACHE_TTL, json.dumps(data))


def invalidate_device_cache(serial_code: int) -> None:
    """Инвалидирует кэш устройства — вызывается из consumer при обновлении/удалении."""
    redis_client.delete(f"device:{serial_code}")


@app.post(
    "/ingest/",
    response_model=schemas.TelemetryResponse,
    dependencies=[Depends(check_rate_limit)],
    summary="Принять показатель от устройства",
)
def ingest_telemetry(
    payload: schemas.TelemetryIngest,
    db: Session = Depends(get_db),
):
    device_data = get_device_from_cache(payload.serial_code)

    if device_data is None:
        db_device = db.query(models.Device).filter(
            models.Device.serial_code == payload.serial_code
        ).first()

        if db_device is None:
            raise HTTPException(status_code=404, detail="Device not found")

        set_device_cache(db_device)
        device_data = {
            "id": db_device.id,
            "serial_code": db_device.serial_code,
            "name": db_device.name,
            "type": db_device.type,
            "location": db_device.location,
            "is_active": db_device.is_active,
        }

    if not device_data["is_active"]:
        raise HTTPException(status_code=403, detail="Device is inactive")

    ts = payload.timestamp or datetime.now(timezone.utc)
    ts_iso = ts.isoformat()

    send_telemetry_event({
        "device_id": device_data["id"],
        "serial_code": device_data["serial_code"],
        "device_name": device_data["name"],
        "device_type": device_data["type"],
        "location": device_data["location"],
        "metric": payload.metric,
        "value": payload.value,
        "unit": payload.unit,
        "timestamp": ts_iso,
    })

    return schemas.TelemetryResponse(
        status="ok",
        device_id=device_data["id"],
        serial_code=device_data["serial_code"],
        metric=payload.metric,
        value=payload.value,
        timestamp=ts_iso,
    )


@app.get("/health")
def health():
    return {"status": "ok"}
