from datetime import datetime, timezone, timedelta
from typing import Optional
from fastapi import FastAPI, HTTPException, Query
from influxdb_client.client.exceptions import InfluxDBError

import schemas

from influx_client import query_api, INFLUX_BUCKET, INFLUX_ORG

app = FastAPI(title="IoT History Service", root_path="/hist")


def _parse_window(start: Optional[str], stop: Optional[str], last_hours: int) -> tuple[str, str]:
    """
    Возвращает (start, stop) в формате RFC3339.
    Если start/stop не заданы — берём последние last_hours часов.
    """
    now = datetime.now(timezone.utc)
    if start and stop:
        return start, stop
    stop_dt  = now
    start_dt = now - timedelta(hours=last_hours)
    return start_dt.isoformat(), stop_dt.isoformat()


@app.get(
    "/history/{device_id}",
    response_model=schemas.HistoryResponse,
    summary="История показателей устройства",
)
def get_device_history(
    device_id: int,
    metric: str = Query(..., description="Название метрики: temperature, humidity, ..."),
    start: Optional[str] = Query(None, description="RFC3339 начало диапазона"),
    stop:  Optional[str] = Query(None, description="RFC3339 конец диапазона"),
    last_hours: int       = Query(24,   description="Глубина выборки в часах (если start/stop не заданы)"),
):
    start_str, stop_str = _parse_window(start, stop, last_hours)

    flux = f"""
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: {start_str}, stop: {stop_str})
          |> filter(fn: (r) => r._measurement == "{metric}")
          |> filter(fn: (r) => r.device_id == "{device_id}")
          |> filter(fn: (r) => r._field == "value")
          |> sort(columns: ["_time"])
    """

    try:
        tables = query_api.query(flux, org=INFLUX_ORG)
    except InfluxDBError as e:
        raise HTTPException(status_code=502, detail=f"InfluxDB error: {e}")

    points: list[schemas.TelemetryPoint] = []
    for table in tables:
        for record in table.records:
            points.append(schemas.TelemetryPoint(
                timestamp=record.get_time(),
                device_id=int(record.values.get("device_id", device_id)),
                serial_code=int(record.values.get("serial_code", 0)),
                metric=metric,
                value=record.get_value(),
                unit=record.values.get("unit"),
                location=record.values.get("location"),
            ))

    return schemas.HistoryResponse(
        device_id=device_id,
        metric=metric,
        start=start_str,
        stop=stop_str,
        points=points,
    )


@app.get(
    "/history/{device_id}/metrics",
    summary="Список метрик устройства за период",
)
def get_device_metrics(
    device_id: int,
    last_hours: int = Query(24, description="Глубина поиска в часах"),
):
    """Возвращает список метрик, по которым есть данные для устройства."""
    now       = datetime.now(timezone.utc)
    start_str = (now - timedelta(hours=last_hours)).isoformat()
    stop_str  = now.isoformat()

    flux = f"""
        import "influxdata/influxdb/schema"
        schema.measurementTagValues(
          bucket: "{INFLUX_BUCKET}",
          measurement: /./,
          tag: "device_id",
          start: {start_str},
          stop: {stop_str}
        )
        |> filter(fn: (r) => r._value == "{device_id}")
    """
    # Проще и надёжнее: список уникальных measurements через общий запрос
    flux_simple = f"""
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: {start_str}, stop: {stop_str})
          |> filter(fn: (r) => r.device_id == "{device_id}")
          |> filter(fn: (r) => r._field == "value")
          |> keep(columns: ["_measurement"])
          |> distinct(column: "_measurement")
    """

    try:
        tables = query_api.query(flux_simple, org=INFLUX_ORG)
    except InfluxDBError as e:
        raise HTTPException(status_code=502, detail=f"InfluxDB error: {e}")

    metrics = [
        record.get_value()
        for table in tables
        for record in table.records
    ]
    return {"device_id": device_id, "metrics": metrics}


@app.get("/health")
def health():
    return {"status": "ok"}
