import os

from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

INFLUX_URL = os.getenv("INFLUXDB_URL", "http://localhost:8086")
INFLUX_TOKEN = os.getenv("INFLUXDB_TOKEN", "")
INFLUX_ORG = os.getenv("INFLUXDB_ORG", "smart_city")
INFLUX_BUCKET = os.getenv("INFLUXDB_BUCKET", "telemetry")

_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)

write_api = _client.write_api(write_options=SYNCHRONOUS)
query_api = _client.query_api()
