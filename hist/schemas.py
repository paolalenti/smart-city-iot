from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime


class TelemetryPoint(BaseModel):
    timestamp: datetime
    device_id: int
    serial_code: int
    metric: str
    value: float
    unit: Optional[str] = None
    location: Optional[str] = None


class HistoryResponse(BaseModel):
    device_id: int
    metric: str
    start: str
    stop: str
    points: List[TelemetryPoint]
