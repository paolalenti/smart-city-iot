from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class DeviceCreate(BaseModel):
    name: str
    type: str
    location: Optional[str] = None


class Device(DeviceCreate):
    id: int
    is_active: bool
    created_at: datetime

    class Config:
        from_attributes = True


class DeviceUpdate(BaseModel):
    name: Optional[str] = None
    type: Optional[str] = None
    location: Optional[str] = None
    is_active: Optional[bool] = None
