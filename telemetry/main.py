import os
from datetime import datetime, timezone

import redis
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


def check_rate_limit(request: Request):
    ip = request.client.host
    key = f"rate_limit:telemetry_service:{ip}"
    current = redis_client.incr(key)
    if current == 1:
        redis_client.expire(key, RATE_LIMIT_WINDOW)
    if current > RATE_LIMIT:
        raise HTTPException(status_code=429, detail="Too Many Requests")


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
    device = db.query(models.Device).filter(
        models.Device.serial_code == payload.serial_code
    ).first()

    if device is None:
        raise HTTPException(status_code=404, detail="Device not found")
    if not device.is_active:
        raise HTTPException(status_code=403, detail="Device is inactive")

    ts = payload.timestamp or datetime.now(timezone.utc)
    ts_iso = ts.isoformat()

    send_telemetry_event({
        "device_id": device.id,
        "serial_code": device.serial_code,
        "device_name": device.name,
        "device_type": device.type,
        "location": device.location,
        "metric": payload.metric,
        "value": payload.value,
        "unit": payload.unit,
        "timestamp": ts_iso,
    })

    return schemas.TelemetryResponse(
        status="ok",
        device_id=device.id,
        serial_code=device.serial_code,
        metric=payload.metric,
        value=payload.value,
        timestamp=ts_iso,
    )


@app.get("/health")
def health():
    return {"status": "ok"}
