-- ============================================================
-- Snowflake Setup Script
-- Run this manually in your Snowflake worksheet BEFORE
-- starting the pipeline with Snowflake credentials.
-- ============================================================

-- 1. Create database + schema
CREATE DATABASE IF NOT EXISTS IOT_DB;
CREATE SCHEMA IF NOT EXISTS IOT_DB.PUBLIC;
USE DATABASE IOT_DB;
USE SCHEMA PUBLIC;

-- 2. Create warehouse (auto-suspend to control cost)
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

-- 3. Raw sensor readings table
CREATE TABLE IF NOT EXISTS SENSOR_READINGS (
    DEVICE_ID     VARCHAR(64)   NOT NULL,
    LOCATION      VARCHAR(128),
    DEVICE_TYPE   VARCHAR(64),
    SENSOR_TYPE   VARCHAR(64)   NOT NULL,
    VALUE         FLOAT         NOT NULL,
    UNIT          VARCHAR(16),
    TIMESTAMP     TIMESTAMP_TZ  NOT NULL,
    QUALITY       VARCHAR(32),
    MQTT_TOPIC    VARCHAR(256),
    INGEST_TIME   TIMESTAMP_TZ  DEFAULT CURRENT_TIMESTAMP()
);

-- Cluster key for efficient time-range queries
ALTER TABLE SENSOR_READINGS CLUSTER BY (SENSOR_TYPE, DATE(TIMESTAMP));

-- 4. Aggregations table
CREATE TABLE IF NOT EXISTS SENSOR_AGGREGATIONS (
    WINDOW_START   TIMESTAMP_TZ  NOT NULL,
    WINDOW_END     TIMESTAMP_TZ  NOT NULL,
    DEVICE_ID      VARCHAR(64)   NOT NULL,
    SENSOR_TYPE    VARCHAR(64)   NOT NULL,
    LOCATION       VARCHAR(128),
    AVG_VALUE      FLOAT,
    MIN_VALUE      FLOAT,
    MAX_VALUE      FLOAT,
    STDDEV_VALUE   FLOAT,
    READING_COUNT  NUMBER(18,0)
);

-- 5. Example analytical queries (useful for portfolio demo)

-- Hourly average temperature per device (last 7 days)
CREATE OR REPLACE VIEW HOURLY_TEMPERATURE AS
SELECT
    DATE_TRUNC('hour', TIMESTAMP) AS hour_bucket,
    DEVICE_ID,
    LOCATION,
    AVG(VALUE)    AS avg_temp,
    MIN(VALUE)    AS min_temp,
    MAX(VALUE)    AS max_temp,
    COUNT(*)      AS readings
FROM SENSOR_READINGS
WHERE SENSOR_TYPE = 'temperature'
  AND TIMESTAMP >= DATEADD(day, -7, CURRENT_TIMESTAMP())
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 2;

-- Anomaly detection: readings > 2 std devs from device mean
CREATE OR REPLACE VIEW ANOMALIES AS
WITH stats AS (
    SELECT
        DEVICE_ID,
        SENSOR_TYPE,
        AVG(VALUE) AS mean_val,
        STDDEV(VALUE) AS std_val
    FROM SENSOR_READINGS
    GROUP BY DEVICE_ID, SENSOR_TYPE
)
SELECT
    r.DEVICE_ID,
    r.SENSOR_TYPE,
    r.LOCATION,
    r.VALUE,
    s.mean_val,
    s.std_val,
    ABS(r.VALUE - s.mean_val) / NULLIF(s.std_val, 0) AS z_score,
    r.TIMESTAMP
FROM SENSOR_READINGS r
JOIN stats s ON r.DEVICE_ID = s.DEVICE_ID AND r.SENSOR_TYPE = s.SENSOR_TYPE
WHERE ABS(r.VALUE - s.mean_val) / NULLIF(s.std_val, 0) > 2
ORDER BY r.TIMESTAMP DESC;

-- 6. Role + user for the pipeline (optional, use in production)
-- CREATE ROLE IOT_PIPELINE_ROLE;
-- GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE IOT_PIPELINE_ROLE;
-- GRANT ALL ON DATABASE IOT_DB TO ROLE IOT_PIPELINE_ROLE;
-- CREATE USER IOT_SERVICE_USER PASSWORD='...' DEFAULT_ROLE=IOT_PIPELINE_ROLE;
-- GRANT ROLE IOT_PIPELINE_ROLE TO USER IOT_SERVICE_USER;