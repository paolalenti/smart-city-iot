from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime


class TelemetryIngest(BaseModel):
    """Входящий payload от Gateway."""
    serial_code: int
    metric: str
    value: float
    unit: Optional[str] = None
    timestamp: Optional[datetime] = None  # если None — проставляется сервером


class TelemetryEvent(BaseModel):
    """Payload, публикуемый в топик telemetry."""
    device_id: int
    serial_code: int
    device_name: str
    device_type: str
    location: Optional[str]
    metric: str
    value: float
    unit: Optional[str]
    timestamp: str  # ISO-8601


class TelemetryResponse(BaseModel):
    status: str
    device_id: int
    serial_code: int
    metric: str
    value: float
    timestamp: str
