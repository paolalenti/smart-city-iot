import os
import redis

from fastapi import FastAPI, Depends, HTTPException, Request
from sqlalchemy.orm import Session
from typing import List
from producer import send_device_event

import models, schemas

from database import engine, get_db

models.Base.metadata.create_all(bind=engine)

app = FastAPI(title="IoT Device Manager", root_path="/device_manager")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

redis_client = redis.Redis(
    host=REDIS_HOST, port=REDIS_PORT,
    db=0, decode_responses=True
)

RATE_LIMIT = 5
RATE_LIMIT_WINDOW = 60


def check_rate_limit(request: Request):
    ip = request.client.host
    key = f"rate_limit:{ip}"
    current_requests = redis_client.incr(key)
    if current_requests == 1:
        redis_client.expire(key, RATE_LIMIT_WINDOW)
    if current_requests > RATE_LIMIT:
        raise HTTPException(
            status_code=429,
            detail="Too Many Requests. Please wait before registering new devices."
        )


# ── Units ─────────────────────────────────────────────────────────────────────

@app.post("/units/", response_model=schemas.Unit, dependencies=[Depends(check_rate_limit)])
def create_unit(unit: schemas.UnitCreate, db: Session = Depends(get_db)):
    if db.query(models.Unit).filter(models.Unit.uid == unit.uid).first():
        raise HTTPException(status_code=409, detail="Unit with this uid already exists")

    db_unit = models.Unit(**unit.model_dump())
    db.add(db_unit)
    db.commit()
    db.refresh(db_unit)

    send_device_event("unit_created", {
        "id": db_unit.id,
        "uid": db_unit.uid,
        "name": db_unit.name,
        "location": db_unit.location,
        "is_active": db_unit.is_active,
        "created_at": db_unit.created_at.isoformat(),
    })
    return db_unit


@app.get("/units/", response_model=List[schemas.Unit])
def get_units(db: Session = Depends(get_db)):
    return db.query(models.Unit).all()


@app.get("/units/{unit_id}", response_model=schemas.Unit)
def get_unit(unit_id: int, db: Session = Depends(get_db)):
    db_unit = db.query(models.Unit).filter(models.Unit.id == unit_id).first()
    if db_unit is None:
        raise HTTPException(status_code=404, detail="Unit not found")
    return db_unit


@app.patch("/units/{unit_id}", response_model=schemas.Unit)
def update_unit(unit_id: int, unit_update: schemas.UnitUpdate, db: Session = Depends(get_db)):
    db_unit = db.query(models.Unit).filter(models.Unit.id == unit_id).first()
    if db_unit is None:
        raise HTTPException(status_code=404, detail="Unit not found")

    for key, value in unit_update.model_dump(exclude_unset=True).items():
        setattr(db_unit, key, value)

    db.commit()
    db.refresh(db_unit)
    return db_unit


@app.delete("/units/{unit_id}")
def delete_unit(unit_id: int, db: Session = Depends(get_db)):
    db_unit = db.query(models.Unit).filter(models.Unit.id == unit_id).first()
    if db_unit is None:
        raise HTTPException(status_code=404, detail="Unit not found")

    db.delete(db_unit)
    db.commit()

    send_device_event("unit_deleted", {"unit_id": unit_id})
    return {"message": f"Unit {unit_id} has been deleted successfully"}


# ── Devices ───────────────────────────────────────────────────────────────────

@app.post("/devices/", response_model=schemas.Device, dependencies=[Depends(check_rate_limit)])
def create_device(device: schemas.DeviceCreate, db: Session = Depends(get_db)):
    if device.unit_id is not None:
        if not db.query(models.Unit).filter(models.Unit.id == device.unit_id).first():
            raise HTTPException(status_code=404, detail="Unit not found")

    db_device = models.Device(**device.model_dump())
    db.add(db_device)
    db.commit()
    db.refresh(db_device)

    send_device_event("created", {
        "id": db_device.id,
        "serial_code": db_device.serial_code,
        "name": db_device.name,
        "type": db_device.type,
        "location": db_device.location,
        "api_endpoint": db_device.api_endpoint,
        "is_active": db_device.is_active,
        "unit_id": db_device.unit_id,
        "created_at": db_device.created_at.isoformat(),
    })
    return db_device


@app.get("/devices/", response_model=List[schemas.Device])
def get_devices(db: Session = Depends(get_db)):
    return db.query(models.Device).all()


@app.get("/devices/{device_id}", response_model=schemas.Device)
def get_device(device_id: int, db: Session = Depends(get_db)):
    db_device = db.query(models.Device).filter(models.Device.id == device_id).first()
    if db_device is None:
        raise HTTPException(status_code=404, detail="Device not found")
    return db_device


@app.patch("/devices/{device_id}", response_model=schemas.Device)
def update_device(device_id: int, device_update: schemas.DeviceUpdate, db: Session = Depends(get_db)):
    db_device = db.query(models.Device).filter(models.Device.id == device_id).first()
    if db_device is None:
        raise HTTPException(status_code=404, detail="Device not found")

    update_data = device_update.model_dump(exclude_unset=True)

    if "unit_id" in update_data and update_data["unit_id"] is not None:
        if not db.query(models.Unit).filter(models.Unit.id == update_data["unit_id"]).first():
            raise HTTPException(status_code=404, detail="Unit not found")

    for key, value in update_data.items():
        setattr(db_device, key, value)

    db.commit()
    db.refresh(db_device)
    return db_device


@app.delete("/devices/{device_id}")
def delete_device(device_id: int, db: Session = Depends(get_db)):
    db_device = db.query(models.Device).filter(models.Device.id == device_id).first()
    if db_device is None:
        raise HTTPException(status_code=404, detail="Device not found")

    db.delete(db_device)
    db.commit()

    send_device_event("deleted", {"device_id": device_id})
    return {"message": f"Device {device_id} has been deleted successfully"}
