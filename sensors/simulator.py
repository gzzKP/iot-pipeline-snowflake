"""
IoT Sensor Simulator
Publishes synthetic sensor readings to MQTT broker.

Topics:
  sensors/temperature/<device_id>
  sensors/humidity/<device_id>
  sensors/pressure/<device_id>
  sensors/vibration/<device_id>
"""
import json
import math
import os
import random
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt


BROKER_HOST = os.getenv("MQTT_BROKER_HOST", "localhost")
BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", "1883"))
INTERVAL = float(os.getenv("PUBLISH_INTERVAL_SECONDS", "2"))

## Simulated device fleet
DEVICES = [
    {"id": "device-001", "location": "factory-floor-A", "type": "industrial"},
    {"id": "device-002", "location": "factory-floor-B", "type": "industrial"},
    {"id": "device-003", "location": "warehouse-1",    "type": "environmental"},
    {"id": "device-004", "location": "warehouse-2",    "type": "environmental"},
    {"id": "device-005", "location": "rooftop",        "type": "weather"},
]

## Sensor models 
def read_temperature(device: dict, t: float) -> float:
    """Simulate temperature with diurnal cycle + noise."""
    base = 22.0 if device["type"] == "environmental" else 45.0
    cycle = 3.0 * math.sin(2 * math.pi * t / 86400)
    noise = random.gauss(0, 0.5)
    # Occasional anomaly spike
    spike = random.choices([0, random.uniform(5, 15)], weights=[0.98, 0.02])[0]
    return round(base + cycle + noise + spike, 2)

def read_humidity(device: dict, t: float) -> float:
    base = 60.0 if device["type"] != "industrial" else 35.0
    noise = random.gauss(0, 1.5)
    return round(max(0, min(100, base + noise)), 2)

def read_pressure(device: dict) -> float:
    base = 1013.25
    return round(base + random.gauss(0, 2), 2)

def read_vibration(device: dict) -> float:
    """Vibration only meaningful for industrial devices."""
    if device["type"] != "industrial":
        return 0.0
    base = 0.12
    # Simulate machine load bursts
    burst = random.choices([0, random.uniform(0.5, 2.0)], weights=[0.95, 0.05])[0]
    return round(base + abs(random.gauss(0, 0.05)) + burst, 4)

## MQTT callbacks
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print(f"[simulator] Connected to MQTT broker at {BROKER_HOST}:{BROKER_PORT}")
    else:
        print(f"[simulator] Connection failed with code {rc}")

## Main loop
def build_payload(device: dict, sensor_type: str, value: float) -> dict:
    return {
        "device_id":   device["id"],
        "location":    device["location"],
        "device_type": device["type"],
        "sensor_type": sensor_type,
        "value":       value,
        "unit":        {"temperature": "°C", "humidity": "%", "pressure": "hPa", "vibration": "g"}[sensor_type],
        "timestamp":   datetime.now(timezone.utc).isoformat(),
        "quality":     random.choices(["good", "uncertain"], weights=[0.95, 0.05])[0],
    }

def main():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.connect(BROKER_HOST, BROKER_PORT, 60)
    client.loop_start()

    # Wait for connection
    time.sleep(3)

    print(f"[simulator] Publishing every {INTERVAL}s for {len(DEVICES)} devices…")
    msg_count = 0

    while True:
        t = time.time()
        for device in DEVICES:
            readings = [
                ("temperature", read_temperature(device, t)),
                ("humidity",    read_humidity(device, t)),
                ("pressure",    read_pressure(device)),
                ("vibration",   read_vibration(device)),
            ]
            for sensor_type, value in readings:
                topic = f"sensors/{sensor_type}/{device['id']}"
                payload = build_payload(device, sensor_type, value)
                client.publish(topic, json.dumps(payload), qos=1)
                msg_count += 1

        if msg_count % 100 == 0:
            print(f"[simulator] {msg_count} messages published")

        time.sleep(INTERVAL)

if __name__ == "__main__":
    main()
