-- Raw sensor readings (ReplacingMergeTree for idempotent JDBC writes)
CREATE TABLE IF NOT EXISTS iot.sensor_readings
(
    device_id   String,
    location    String,
    device_type String,
    sensor_type String,
    value       Float64,
    unit        String,
    timestamp   DateTime,
    quality     String,
    mqtt_topic  String,
    ingest_time DateTime DEFAULT now64()
)
ENGINE = ReplacingMergeTree(ingest_time)
PARTITION BY (toYYYYMMDD(timestamp), sensor_type)
ORDER BY (device_id, sensor_type, timestamp)
TTL timestamp + INTERVAL 30 DAY;

-- Window aggregations (5-minute tumbling windows)
CREATE TABLE IF NOT EXISTS iot.sensor_aggregations
(
    window_start  DateTime64(3, 'UTC'),
    window_end    DateTime64(3, 'UTC'),
    device_id     String,
    sensor_type   String,
    location      String,
    avg_value     Float64,
    min_value     Float64,
    max_value     Float64,
    stddev_value  Float64,
    reading_count UInt64
)
ENGINE = SummingMergeTree()
PARTITION BY (toYYYYMMDD(window_start))
ORDER BY (device_id, sensor_type, window_start);

-- Materialized view: latest reading per device+sensor
CREATE MATERIALIZED VIEW IF NOT EXISTS iot.device_latest_mv
ENGINE = AggregatingMergeTree()
ORDER BY (device_id, sensor_type)
AS
SELECT
    device_id,
    sensor_type,
    location,
    argMaxState(value, timestamp)     AS latest_value,
    argMaxState(timestamp, timestamp) AS latest_ts,
    maxState(value)                   AS max_value,
    minState(value)                   AS min_value,
    countState()                      AS total_readings
FROM iot.sensor_readings
GROUP BY device_id, sensor_type, location;

-- Convenience view over materialized data
CREATE VIEW IF NOT EXISTS iot.device_current AS
SELECT
    device_id,
    sensor_type,
    location,
    argMaxMerge(latest_value) AS current_value,
    argMaxMerge(latest_ts)    AS last_seen,
    maxMerge(max_value)       AS max_value,
    minMerge(min_value)       AS min_value,
    countMerge(total_readings) AS total_readings
FROM iot.device_latest_mv
GROUP BY device_id, sensor_type, location;