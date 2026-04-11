from sqlalchemy import Column, Integer, String, Boolean, DateTime
from sqlalchemy.sql import func
from database import Base

class Device(Base):
    __tablename__ = "devices"

    id = Column(Integer, primary_key=True, index=True)
    serial_code = Column(Integer, unique=True, nullable=False, index=True)
    name = Column(String, nullable=False)
    type = Column(String, nullable=False)        # "temperature", "humidity", etc.
    location = Column(String)
    api_endpoint = Column(String)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
