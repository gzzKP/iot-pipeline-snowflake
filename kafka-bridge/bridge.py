"""
MQTT → Kafka Bridge
Subscribes to all sensor topics on Mosquitto and forwards
every message as a Kafka record (topic key = MQTT topic path).
"""
import json
import os
import time

import paho.mqtt.client as mqtt
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic


MQTT_HOST   = os.getenv("MQTT_BROKER_HOST", "localhost")
MQTT_PORT   = int(os.getenv("MQTT_BROKER_PORT", "1883"))
MQTT_TOPIC  = os.getenv("MQTT_TOPIC", "sensors/#")

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC   = os.getenv("KAFKA_TOPIC", "iot-sensor-data")


def ensure_topic(servers: str, topic: str, partitions: int = 3):
    admin = AdminClient({"bootstrap.servers": servers})
    meta  = admin.list_topics(timeout=10)
    if topic not in meta.topics:
        new_topic = NewTopic(topic, num_partitions=partitions, replication_factor=1)
        fs = admin.create_topics([new_topic])
        for t, f in fs.items():
            try:
                f.result()
                print(f"[bridge] Created Kafka topic: {t}")
            except Exception as e:
                print(f"[bridge] Topic creation note: {e}")

def delivery_report(err, msg):
    if err:
        print(f"[bridge] Kafka delivery error: {err}")

producer = Producer({
    "bootstrap.servers": KAFKA_SERVERS,
    "linger.ms":         10,
    "batch.size":        16384,
    "compression.type":  "lz4",
    "acks":              "1",
})


def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        client.subscribe(MQTT_TOPIC, qos=1)
        print(f"[bridge] Subscribed to MQTT topic: {MQTT_TOPIC}")
    else:
        print(f"[bridge] MQTT connection failed: rc={rc}")

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode("utf-8")
        # Add the original MQTT topic into the record for lineage
        data = json.loads(payload)
        data["mqtt_topic"] = msg.topic

        producer.produce(
            KAFKA_TOPIC,
            key=msg.topic.encode("utf-8"),
            value=json.dumps(data).encode("utf-8"),
            callback=delivery_report,
        )
        producer.poll(0)
    except Exception as e:
        print(f"[bridge] Error forwarding message: {e}")

def on_disconnect(client, userdata, rc, properties=None, reasoncode=None):
    print(f"[bridge] MQTT disconnected (rc={rc}), will reconnect…")


def main():
    # Wait for Kafka to be ready
    time.sleep(10)
    ensure_topic(KAFKA_SERVERS, KAFKA_TOPIC)

    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqtt_client.on_connect    = on_connect
    mqtt_client.on_message    = on_message
    mqtt_client.on_disconnect = on_disconnect
    mqtt_client.reconnect_delay_set(min_delay=1, max_delay=30)
    mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)

    print(f"[bridge] Starting bridge: MQTT {MQTT_HOST}:{MQTT_PORT} → Kafka {KAFKA_SERVERS}")
    mqtt_client.loop_forever()

if __name__ == "__main__":
    main()
