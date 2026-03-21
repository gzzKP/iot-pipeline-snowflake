# IoT Real-Time Data Pipeline

A full end-to-end portfolio project demonstrating a production-grade IoT streaming
architecture using industry-standard open-source tools.

```
IoT Sensors → MQTT (Mosquitto) → Kafka → Apache Spark Streaming → ClickHouse
                                                                 ↘ Snowflake
```

## Stack

| Layer | Technology | Role |
|---|---|---|
| Messaging | Eclipse Mosquitto | MQTT broker |
| Buffering | Apache Kafka | Durable message queue |
| Processing | Apache Spark 3.5 | Structured streaming + windowed aggregations |
| OLAP (real-time) | ClickHouse 24.3 | Sub-second analytical queries |
| Data warehouse | Snowflake | Historical analytics + ML feature store |
| Orchestration | Docker Compose | Local-first, single-command startup |

## Quickstart

### Prerequisites
- Docker Desktop ≥ 4.x (with Compose v2)
- 8 GB RAM allocated to Docker
- Optional: Snowflake free trial account

### 1. Clone and configure

```bash
git clone https://github.com/<you>/iot-pipeline.git
cd iot-pipeline
cp .env.example .env
# Edit .env with your Snowflake credentials (or leave blank to skip Snowflake)
```

### 2. (Snowflake only) Run the DDL

Open `snowflake/setup.sql` in your Snowflake worksheet and run it once.

### 3. Start the stack

```bash
docker compose up --build -d
```

Services start in this order:
1. Mosquitto (MQTT broker)
2. Zookeeper + Kafka
3. MQTT→Kafka bridge
4. Sensor simulator (starts publishing immediately)
5. ClickHouse
6. Spark (waits 30 s for Kafka to settle, then starts streaming)

### 4. Verify the pipeline

```bash
# Watch Kafka messages arriving
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic iot-sensor-data \
  --from-beginning

# Query ClickHouse - should populate within ~30s of Spark starting
docker exec -it clickhouse clickhouse-client \
  --user iot_user --password iot_pass \
  --query "SELECT sensor_type, count(), avg(value) FROM iot.sensor_readings GROUP BY sensor_type"

# Live dashboard (last 100 readings)
docker exec -it clickhouse clickhouse-client \
  --user iot_user --password iot_pass \
  --query "SELECT device_id, sensor_type, value, timestamp FROM iot.sensor_readings ORDER BY timestamp DESC LIMIT 20"
```

### 5. Useful ClickHouse queries

```sql
-- Current reading per device
SELECT * FROM iot.device_current;

-- 5-minute aggregations
SELECT * FROM iot.sensor_aggregations ORDER BY window_start DESC LIMIT 20;

-- Anomaly detection (z-score > 2)
SELECT device_id, sensor_type, value, timestamp
FROM iot.sensor_readings
WHERE sensor_type = 'temperature'
ORDER BY timestamp DESC;
```

## Architecture Notes

### Why MQTT → Kafka (not MQTT → Spark directly)?

MQTT is designed for device-to-broker messaging, not for distributed consumers.
Kafka adds:
- **Durability**: messages survive broker restarts
- **Fan-out**: multiple consumers (Spark, future dashboards) without changing devices
- **Back-pressure**: Spark can lag behind during GC without losing messages

### Why both ClickHouse and Snowflake?

| | ClickHouse | Snowflake |
|---|---|---|
| Latency | Sub-second | 1-5 seconds |
| Best for | Real-time dashboards | Historical analysis, ML |
| Cost model | Self-hosted | Pay-per-query |
| Local-friendly | Yes (Docker) | Cloud-only |

### Spark checkpoint strategy

Two independent streaming queries run in parallel:
1. **Raw stream** (10 s trigger) → ClickHouse + Snowflake
2. **Aggregation stream** (30 s trigger) → ClickHouse `sensor_aggregations`

Checkpoints are stored at `/tmp/spark-checkpoints/` inside the container.
Mount a volume if you want them to survive restarts.

### Data quality

The Spark job filters out:
- Readings with `quality != "good"`
- Out-of-range values (temperature outside −40–125°C, etc.)
- Null values

Rejected rows are dropped (extend with a dead-letter Kafka topic for production).

## Extending the project

- **Grafana dashboard**: expose ClickHouse HTTP on 8123 and add as a data source
- **Schema registry**: add Confluent Schema Registry for Avro-encoded messages
- **dbt**: add dbt models on top of Snowflake for the analytics layer
- **Kubernetes**: replace Docker Compose with Helm charts for production
- **AlertManager**: set threshold alerts in ClickHouse using scheduled queries

## Stopping

```bash
docker compose down         # stop containers, keep volumes
docker compose down -v      # stop + delete all data volumes
```

## Project structure

```
iot-pipeline/
├── docker-compose.yml      # Full stack orchestration
├── .env.example            # Snowflake credentials template
├── mqtt/
│   └── mosquitto.conf      # MQTT broker config
├── sensors/
│   ├── simulator.py        # IoT device simulator (5 devices, 4 sensors each)
│   └── Dockerfile
├── kafka-bridge/
│   ├── bridge.py           # MQTT subscriber → Kafka producer
│   ├── requirements.txt
│   └── Dockerfile
├── spark/
│   ├── pipeline.py         # Structured Streaming job (core logic)
│   ├── requirements.txt
│   └── Dockerfile
├── clickhouse/
│   └── initdb/
│       └── 01_schema.sql   # Tables, materialized views, TTL
└── snowflake/
    └── setup.sql           # DDL + analytical views
```