-- SmokeStream warehouse: star schema for history, both pipelines' outputs, and context data.
-- Run once in a Snowflake worksheet. Parquet from data/out/ is loaded by the Airflow
-- load DAG (COPY INTO from a stage); this file only defines the tables.

CREATE DATABASE IF NOT EXISTS SMOKESTREAM;
CREATE SCHEMA IF NOT EXISTS SMOKESTREAM.CORE;
USE SCHEMA SMOKESTREAM.CORE;

-- ---------- Dimensions ----------

-- Sensors move, go offline and change reliability over time, so this is a Type 2
-- slowly changing dimension: a new row per change, with validity dates.
CREATE TABLE IF NOT EXISTS DIM_SENSOR (
    SENSOR_KEY        NUMBER AUTOINCREMENT PRIMARY KEY,
    SENSOR_INDEX      NUMBER NOT NULL,          -- PurpleAir's id
    LATITUDE          FLOAT,
    LONGITUDE         FLOAT,
    H3_CELL           VARCHAR(16),
    RELIABILITY_SCORE FLOAT,                    -- nightly score from the Airflow job
    VALID_FROM        TIMESTAMP_NTZ NOT NULL,
    VALID_TO          TIMESTAMP_NTZ,            -- NULL while current
    IS_CURRENT        BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS DIM_H3_CELL (
    H3_CELL      VARCHAR(16) PRIMARY KEY,
    RESOLUTION   NUMBER(2),
    CENTER       GEOGRAPHY,                     -- centroid, for maps and distance queries
    COUNTY       VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS DIM_TIME (
    TIME_KEY     TIMESTAMP_NTZ PRIMARY KEY,     -- start of a 2-minute slot, UTC
    DATE_UTC     DATE,
    HOUR_UTC     NUMBER(2),
    DATE_LOCAL   DATE,                          -- America/Los_Angeles
    HOUR_LOCAL   NUMBER(2),
    DAY_OF_WEEK  VARCHAR(9),
    FIRE_SEASON  VARCHAR(8)                     -- '2020', '2021', '2024' or NULL
);

-- One row per pipeline setting, so every result can be traced to its knobs (RQ2).
CREATE TABLE IF NOT EXISTS DIM_PIPELINE_CONFIG (
    CONFIG_ID              VARCHAR(64) PRIMARY KEY,   -- e.g. w10_wm5_r7, or w10_r7 for batch
    MODE                   VARCHAR(16),               -- 'stream', 'simulated', 'batch'
    WINDOW_MINUTES         NUMBER,
    WATERMARK_MINUTES      NUMBER,                    -- NULL for batch
    QC_HISTORY_MINUTES     NUMBER,
    CONTEXT_REFRESH_MINUTES NUMBER,
    H3_RESOLUTION          NUMBER(2),
    FIRE_SEASON            VARCHAR(8)
);

-- ---------- Facts ----------

-- Grain: one corrected sensor reading (2-minute PurpleAir report).
CREATE TABLE IF NOT EXISTS FACT_READING (
    SENSOR_KEY     NUMBER REFERENCES DIM_SENSOR (SENSOR_KEY),
    TIME_KEY       TIMESTAMP_NTZ REFERENCES DIM_TIME (TIME_KEY),
    EVENT_TS       TIMESTAMP_NTZ,
    PM25_A         FLOAT,
    PM25_B         FLOAT,
    PM1            FLOAT,
    RH             FLOAT,
    PM25_CORRECTED FLOAT,
    PASSED_QC      BOOLEAN
);

-- Grain: one hexagon window produced by one pipeline config. Stream, simulated and
-- batch outputs sit side by side, so RQ1/RQ2 comparisons are a self-join on
-- (H3_CELL, WINDOW_START). Columns match common/enrich.py OUTPUT_COLUMNS.
CREATE TABLE IF NOT EXISTS FACT_HEX_WINDOW (
    CONFIG_ID          VARCHAR(64) REFERENCES DIM_PIPELINE_CONFIG (CONFIG_ID),
    H3_CELL            VARCHAR(16) REFERENCES DIM_H3_CELL (H3_CELL),
    WINDOW_START       TIMESTAMP_NTZ,
    WINDOW_END         TIMESTAMP_NTZ,
    PM25               FLOAT,              -- mean of non-excluded sensors' window means
    PM1                FLOAT,
    AQI                NUMBER,
    AQI_CATEGORY       VARCHAR(32),
    N_READINGS         NUMBER,
    N_SENSORS          NUMBER,
    N_EXCLUDED         NUMBER,             -- sensors dropped by the neighbour check (step 4)
    EXTENT             FLOAT,              -- share of nearby hexagons also elevated
    FINE_RATIO         FLOAT,              -- PM1 / PM2.5
    IN_PLUME           BOOLEAN,
    PLUME_DENSITY      VARCHAR(8),
    WIND_DIR_DEG       FLOAT,
    WIND_SPEED_MS      FLOAT,
    UPWIND_FIRE        BOOLEAN,
    NEAREST_FIRE_KM    FLOAT,
    N_FIRES            NUMBER,
    SMOKE_LABEL        VARCHAR(16),        -- 'wildfire', 'local', 'uncertain'; NULL when not elevated (RQ3)
    SMOKE_ALERT        BOOLEAN,            -- AQI > 150 and not labeled local
    LAST_ARRIVAL_TIME  FLOAT,
    EMITTED_SIM_TIME   FLOAT,              -- simulated runs: when the window would have been emitted
    DECISION_DELAY_S   FLOAT,              -- simulated runs: emitted_sim_time minus window end
    EMITTED_AT         TIMESTAMP_NTZ       -- real streaming runs: wall-clock emit time (RQ4)
);

CREATE TABLE IF NOT EXISTS FACT_ALERT (
    CONFIG_ID       VARCHAR(64) REFERENCES DIM_PIPELINE_CONFIG (CONFIG_ID),
    H3_CELL         VARCHAR(16) REFERENCES DIM_H3_CELL (H3_CELL),
    WINDOW_START    TIMESTAMP_NTZ,
    AQI             NUMBER,
    AQI_CATEGORY    VARCHAR(32),
    SMOKE_LABEL     VARCHAR(16),
    ALERT_SIM_TIME  TIMESTAMP_NTZ          -- when the alert would have fired
);

-- Regulatory monitors (EPA AQS / AirNow), the ground truth.
CREATE TABLE IF NOT EXISTS FACT_MONITOR_HOURLY (
    SITE_ID        VARCHAR(32),
    LATITUDE       FLOAT,
    LONGITUDE      FLOAT,
    H3_CELL        VARCHAR(16),
    HOUR_START_UTC TIMESTAMP_NTZ,
    PM25           FLOAT,
    AQI            NUMBER
);

CREATE TABLE IF NOT EXISTS FACT_FIRE_DETECTION (
    DETECTED_AT  TIMESTAMP_NTZ,
    PUBLISHED_AT TIMESTAMP_NTZ,            -- when it became available (context-freshness knob)
    LATITUDE     FLOAT,
    LONGITUDE    FLOAT,
    H3_CELL      VARCHAR(16),
    FRP_MW       FLOAT                     -- fire radiative power
);

-- Plumes stored as the H3 cells they cover, the same shape the pipeline joins on.
CREATE TABLE IF NOT EXISTS FACT_PLUME_CELL (
    H3_CELL      VARCHAR(16),
    VALID_START  TIMESTAMP_NTZ,
    VALID_END    TIMESTAMP_NTZ,
    DENSITY      VARCHAR(8),               -- Light, Medium, Heavy
    PUBLISHED_AT TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS FACT_WIND_OBS (
    STATION_ID    VARCHAR(16),
    LATITUDE      FLOAT,
    LONGITUDE     FLOAT,
    OBSERVED_AT   TIMESTAMP_NTZ,
    PUBLISHED_AT  TIMESTAMP_NTZ,
    WIND_DIR_DEG  FLOAT,                   -- direction the wind blows FROM
    WIND_SPEED_MS FLOAT
);

-- Example: RQ1 accuracy of one streaming setting against the hindsight baseline.
-- SELECT 'w10_wm5_r7'                                    AS STREAM_CONFIG,
--        SQRT(AVG(POWER(s.PM25 - b.PM25, 2)))            AS RMSE,
--        COUNT(s.PM25) / COUNT(*)                         AS COVERAGE
-- FROM FACT_HEX_WINDOW b
-- LEFT JOIN FACT_HEX_WINDOW s
--   ON s.H3_CELL = b.H3_CELL AND s.WINDOW_START = b.WINDOW_START AND s.CONFIG_ID = 'w10_wm5_r7'
-- WHERE b.CONFIG_ID = 'w10_r7';
