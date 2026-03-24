from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List

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
    return db_device


@app.get("/devices/", response_model=List[schemas.Device])
def get_devices(db: Session = Depends(get_db)):
    devices = db.query(models.Device).all()
    return devices
