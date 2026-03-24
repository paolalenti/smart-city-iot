from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from producer import send_device_event

import models, schemas
from database import engine, get_db


models.Base.metadata.create_all(bind=engine)

app = FastAPI(title="IoT Device Manager")


@app.post("/devices/", response_model=schemas.Device)
def create_device(device: schemas.DeviceCreate, db: Session = Depends(get_db)):
    db_device = models.Device(
        name=device.name,
        type=device.type,
        location=device.location
    )
    db.add(db_device)
    db.commit()
    db.refresh(db_device)

    device_info = {
        "id": db_device.id,
        "name": db_device.name,
        "type": db_device.type,
        "location": db_device.location,
        "is_active": db_device.is_active,
        "created_at": db_device.created_at.isoformat()
    }
    send_device_event("created", device_info)

    return db_device


@app.get("/devices/", response_model=List[schemas.Device])
def get_devices(db: Session = Depends(get_db)):
    devices = db.query(models.Device).all()
    return devices


@app.get("/devices/{device_id}", response_model=schemas.Device)
def get_device(device_id: int, db: Session = Depends(get_db)):
    db_device = db.query(models.Device).filter(models.Device.id == device_id).first()
    if db_device is None:
        raise HTTPException(status_code=404, detail="Device not found")
    return db_device


@app.patch("/devices/{device_id}", response_model=schemas.Device)
def update_device(
        device_id: int, device_update: schemas.DeviceUpdate, db: Session = Depends(get_db)
):
    db_device = db.query(models.Device).filter(models.Device.id == device_id).first()
    if db_device is None:
        raise HTTPException(status_code=404, detail="Device not found")

    update_data = device_update.model_dump(exclude_unset=True)

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
