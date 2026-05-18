import ipaddress
from time import sleep
from locust import HttpUser, task, between
import random

BASE_URL = "https://iot-helm.local"

NUM_DEVICES_TO_REGISTER = 1


def spoof_ip():
    while True:
        ip = ipaddress.IPv4Address(random.randint(0, 2**32 - 1))
        if not ip.is_private and not ip.is_reserved and not ip.is_multicast:
            return {"X-Forwarded-For": str(ip)}


class TestUser(HttpUser):
    wait_time = between(3, 5)
    host = BASE_URL

    def on_start(self) -> None:
        self.headers = spoof_ip()
        self.devices = []
        for i in range(NUM_DEVICES_TO_REGISTER):
            device_data = {
                "serial_code": random.getrandbits(12),
                "name": "test_device",
                "type": "temperature"
            }
            response = self.client.post(f"/device_manager/devices/", json=device_data, verify=False, headers=self.headers)
            if response.status_code == 200:
                print(f"Device {i} registered successfully")
                self.devices.append(response.json())
            else:
                print(f"Error registering device {i}: {response.status_code}, {response.text}")
            sleep(2)

    def on_stop(self):
        # Check if the devices list is available
        if not hasattr(self, 'devices') or not self.devices:
            return

        for device in self.devices:
            device_id = device["id"]
            response = self.client.delete(f"/device_manager/devices/{device_id}/", verify=False, headers=self.headers)
            if response.status_code == 200:
                print(f"Device {device_id} deleted successfully")
                self.devices.append(response.json())
            else:
                print(f"Error deleting device {device_id}: {response.status_code}, {response.text}")

    @task(5)
    def send_telemetry(self):
        # Check if the devices list is available
        if not hasattr(self, 'devices') or not self.devices:
            print("Devices list not available. Register devices before sending telemetry.")
            return

        # Select a random registered device
        device = random.choice(self.devices)
        serial_code = device["serial_code"]
        data = {
            "serial_code": serial_code,
            "metric": "temperature",
            "value": random.uniform(15.0, 45.0)
        }

        response = self.client.post("/telemetry/ingest/", json=data, verify=False, headers=self.headers)
        if response.status_code == 200:
            print(f"Telemetry sent for device {serial_code}")
        else:
            print(f"Error sending telemetry for device {serial_code}: {response.status_code}, {response.text}")

    @task(1)
    def get_device(self):
        if not hasattr(self, 'devices') or not self.devices:
            print("Devices list not available. Register devices before fetching.")
            return

        device = random.choice(self.devices)
        device_id = device["id"]

        response = self.client.get(f"/device_manager/devices/{device_id}/", verify=False, headers=self.headers)
        if response.status_code == 200:
            print(f"Received device info for device {device_id}")
        else:
            print(f"Error getting device info: {response.status_code}")
