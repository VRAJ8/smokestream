-- SmokeStream warehouse: star schema for history, both pipelines' outputs, and context data.
-- warehouse/load_snowflake.py runs this file (setup) and loads the tables (load); it can
-- also be run by hand in a worksheet. Every statement is safe to run again.

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

-- One row per pipeline setting within a run, so every result can be traced to its
-- knobs (RQ2). A run is one output folder: a season sweep under one delay profile, or
-- one live streaming run; the same CONFIG_ID appears in several runs.
CREATE TABLE IF NOT EXISTS DIM_PIPELINE_CONFIG (
    RUN_ID                 VARCHAR(64),               -- e.g. stream_2020-delay-profile-partial, demo_live
    CONFIG_ID              VARCHAR(64),               -- e.g. w10_wm5_q120_c60_r7, or w10_q120_r7 for batch
    MODE                   VARCHAR(16),               -- 'stream', 'simulated', 'batch'
    WINDOW_MINUTES         NUMBER,
    WATERMARK_MINUTES      NUMBER,                    -- NULL for batch
    QC_HISTORY_MINUTES     NUMBER,
    CONTEXT_REFRESH_MINUTES NUMBER,
    H3_RESOLUTION          NUMBER(2),
    FIRE_SEASON            VARCHAR(8),
    PRIMARY KEY (RUN_ID, CONFIG_ID)
);

-- ---------- Facts ----------

-- Grain: one corrected sensor reading (10-minute averages in the history we load;
-- the live feed reports every 2 minutes).
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

-- Grain: one hexagon window produced by one pipeline config in one run. Stream,
-- simulated and batch outputs sit side by side, so RQ1/RQ2 comparisons are a self-join
-- on (H3_CELL, WINDOW_START). Columns match common/enrich.py OUTPUT_COLUMNS.
CREATE TABLE IF NOT EXISTS FACT_HEX_WINDOW (
    RUN_ID             VARCHAR(64),
    CONFIG_ID          VARCHAR(64),        -- with RUN_ID, references DIM_PIPELINE_CONFIG
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
    EMITTED_AT         TIMESTAMP_NTZ,      -- real streaming runs: wall-clock emit time (RQ4)
    FOREIGN KEY (RUN_ID, CONFIG_ID) REFERENCES DIM_PIPELINE_CONFIG (RUN_ID, CONFIG_ID)
);

-- Derived from FACT_HEX_WINDOW by the loader: one row per window that raised a smoke alert.
CREATE TABLE IF NOT EXISTS FACT_ALERT (
    RUN_ID          VARCHAR(64),
    CONFIG_ID       VARCHAR(64),
    H3_CELL         VARCHAR(16) REFERENCES DIM_H3_CELL (H3_CELL),
    WINDOW_START    TIMESTAMP_NTZ,
    AQI             NUMBER,
    AQI_CATEGORY    VARCHAR(32),
    SMOKE_LABEL     VARCHAR(16),
    ALERT_SIM_TIME  TIMESTAMP_NTZ,         -- when the alert would have fired (simulated runs)
    FOREIGN KEY (RUN_ID, CONFIG_ID) REFERENCES DIM_PIPELINE_CONFIG (RUN_ID, CONFIG_ID)
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
