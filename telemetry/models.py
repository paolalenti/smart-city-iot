from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from database import Base


class Unit(Base):
    """Зеркало units из device_manager."""
    __tablename__ = "units_mirror"

    id = Column(Integer, primary_key=True, index=True)
    uid = Column(String, unique=True, nullable=False, index=True)
    name = Column(String, nullable=False)
    location = Column(String)
    is_active = Column(Boolean, default=True)
    synced_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    sensors = relationship("Device", back_populates="unit", lazy="selectin")


class Device(Base):
    """Зеркало devices из device_manager."""
    __tablename__ = "devices_mirror"

    id = Column(Integer, primary_key=True, index=True)
    serial_code = Column(Integer, unique=True, nullable=False, index=True)
    name = Column(String, nullable=False)
    type = Column(String, nullable=False)
    location = Column(String)
    is_active = Column(Boolean, default=True)
    synced_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    unit_id = Column(Integer, ForeignKey("units_mirror.id", ondelete="SET NULL"), nullable=True)
    unit = relationship("Unit", back_populates="sensors")
