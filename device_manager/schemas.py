from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime


# ── Device ────────────────────────────────────────────────────────────────────

class DeviceCreate(BaseModel):
    serial_code: int
    name: str
    type: str
    location: Optional[str] = None
    api_endpoint: Optional[str] = None
    unit_id: Optional[int] = None  # можно создать датчик без привязки к Unit


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
    api_endpoint: Optional[str] = None
    is_active: Optional[bool] = None
    unit_id: Optional[int] = None  # позволяет перепривязать / отвязать датчик


# ── Unit ──────────────────────────────────────────────────────────────────────

class UnitCreate(BaseModel):
    uid: str  # уникальный публичный идентификатор
    name: str
    location: Optional[str] = None
    api_endpoint: Optional[str] = None


class Unit(UnitCreate):
    id: int
    is_active: bool
    created_at: datetime
    sensors: List[Device] = []

    class Config:
        from_attributes = True


class UnitUpdate(BaseModel):
    name: Optional[str] = None
    location: Optional[str] = None
    api_endpoint: Optional[str] = None
    is_active: Optional[bool] = None
