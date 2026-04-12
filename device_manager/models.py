from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from database import Base


class Unit(Base):
    """
    Физическое устройство (контроллер, хаб, edge-узел),
    к которому подключены датчики (Device).
    """
    __tablename__ = "units"

    id = Column(Integer, primary_key=True, index=True)
    uid = Column(String, unique=True, nullable=False, index=True)  # уникальный публичный идентификатор
    name = Column(String, nullable=False)
    location = Column(String)
    api_endpoint = Column(String)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    sensors = relationship("Device", back_populates="unit", lazy="selectin")


class Device(Base):
    """
    Датчик (сенсор), физически подключённый к Unit.
    """
    __tablename__ = "devices"

    id = Column(Integer, primary_key=True, index=True)
    serial_code = Column(Integer, unique=True, nullable=False, index=True)
    name = Column(String, nullable=False)
    type = Column(String, nullable=False)        # "temperature", "humidity", etc.
    location = Column(String)
    api_endpoint = Column(String)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    unit_id = Column(Integer, ForeignKey("units.id", ondelete="SET NULL"), nullable=True, index=True)
    unit = relationship("Unit", back_populates="sensors")
