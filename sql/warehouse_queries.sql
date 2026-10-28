-- Analysis queries on the SmokeStream warehouse (sql/snowflake_schema.sql).
-- Run one with: python warehouse/load_snowflake.py query <name>, or paste into a worksheet
-- after USE SCHEMA SMOKESTREAM.CORE;

-- name: rq1_stream_vs_hindsight
-- RQ1/RQ2: every streaming setting against the hindsight batch baseline of the same run,
-- window length and neighbour-check history. Same definitions as
-- analysis/compare_stream_batch.py; `load_snowflake.py verify` checks that they agree.
WITH pairs AS (
    SELECT s.RUN_ID AS STREAM_RUN, s.CONFIG_ID AS STREAM_CONFIG, b.RUN_ID AS BATCH_RUN, b.CONFIG_ID AS BATCH_CONFIG,
           s.WINDOW_MINUTES, s.WATERMARK_MINUTES, s.QC_HISTORY_MINUTES, s.CONTEXT_REFRESH_MINUTES
    FROM DIM_PIPELINE_CONFIG s
    JOIN DIM_PIPELINE_CONFIG b
      ON b.MODE = 'batch' AND s.MODE <> 'batch'
     AND b.RUN_ID = REPLACE(s.RUN_ID, 'stream_', 'batch_')
     AND b.WINDOW_MINUTES = s.WINDOW_MINUTES
     AND b.QC_HISTORY_MINUTES = s.QC_HISTORY_MINUTES
     AND b.H3_RESOLUTION = s.H3_RESOLUTION
),
cutoff AS (  -- the baseline is compared only up to the stream's last window
    SELECT RUN_ID, CONFIG_ID, MAX(WINDOW_END) AS LAST_END FROM FACT_HEX_WINDOW GROUP BY RUN_ID, CONFIG_ID
)
SELECT p.STREAM_RUN, p.STREAM_CONFIG, p.BATCH_CONFIG,
       p.WINDOW_MINUTES, p.WATERMARK_MINUTES, p.QC_HISTORY_MINUTES, p.CONTEXT_REFRESH_MINUTES,
       COUNT(*)                                                       AS BASELINE_WINDOWS,
       COUNT(s.PM25) / COUNT(*)                                       AS WINDOW_COVERAGE,
       SQRT(AVG(SQUARE(s.PM25 - b.PM25)))                             AS RMSE_UGM3,
       AVG(IFF(b.SMOKE_LABEL IS NULL, NULL, IFF(s.SMOKE_LABEL = b.SMOKE_LABEL, 1, 0))) AS LABEL_MATCH
FROM pairs p
JOIN cutoff c ON c.RUN_ID = p.STREAM_RUN AND c.CONFIG_ID = p.STREAM_CONFIG
JOIN FACT_HEX_WINDOW b
  ON b.RUN_ID = p.BATCH_RUN AND b.CONFIG_ID = p.BATCH_CONFIG AND b.WINDOW_END <= c.LAST_END
LEFT JOIN FACT_HEX_WINDOW s
  ON s.RUN_ID = p.STREAM_RUN AND s.CONFIG_ID = p.STREAM_CONFIG
 AND s.H3_CELL = b.H3_CELL AND s.WINDOW_START = b.WINDOW_START
GROUP BY ALL
ORDER BY p.STREAM_RUN, p.WINDOW_MINUTES, p.WATERMARK_MINUTES, p.QC_HISTORY_MINUTES, p.CONTEXT_REFRESH_MINUTES;

-- name: season_summary
-- Ground truth per season: regulatory monitor hours and Unhealthy air (PM2.5 above 55.4).
SELECT t.FIRE_SEASON,
       COUNT(DISTINCT m.SITE_ID)                                  AS MONITOR_SITES,
       COUNT(m.PM25)                                              AS SITE_HOURS,
       COUNT_IF(m.PM25 > 55.4)                                    AS UNHEALTHY_SITE_HOURS,
       COUNT(DISTINCT IFF(m.PM25 > 55.4, t.DATE_LOCAL, NULL))     AS UNHEALTHY_DAYS_LOCAL,
       MAX(m.PM25)                                                AS PEAK_PM25
FROM FACT_MONITOR_HOURLY m
JOIN DIM_TIME t ON t.TIME_KEY = m.HOUR_START_UTC
GROUP BY t.FIRE_SEASON
ORDER BY t.FIRE_SEASON;

-- name: daily_peak_near_sjsu
-- Smokiest days within 10 km of SJSU (hindsight baseline, 60-minute windows), using the
-- hexagon centers in DIM_H3_CELL and Snowflake's geospatial functions.
SELECT t.FIRE_SEASON, t.DATE_LOCAL,
       ROUND(MAX(h.PM25), 1)        AS PEAK_PM25,
       MAX(h.AQI)                   AS PEAK_AQI,
       COUNT(DISTINCT h.H3_CELL)    AS HEXAGONS
FROM FACT_HEX_WINDOW h
JOIN DIM_PIPELINE_CONFIG p ON p.RUN_ID = h.RUN_ID AND p.CONFIG_ID = h.CONFIG_ID
JOIN DIM_H3_CELL c ON c.H3_CELL = h.H3_CELL
JOIN DIM_TIME t ON t.TIME_KEY = h.WINDOW_START
WHERE p.MODE = 'batch' AND p.WINDOW_MINUTES = 60 AND p.QC_HISTORY_MINUTES = 120
  AND ST_DWITHIN(c.CENTER, ST_MAKEPOINT(-121.8811, 37.3352), 10000)
GROUP BY t.FIRE_SEASON, t.DATE_LOCAL
ORDER BY PEAK_PM25 DESC
LIMIT 10;

-- name: alerts_by_label
-- Smoke alerts per run under the default settings, split by smoke label.
SELECT p.RUN_ID, p.CONFIG_ID, p.MODE, a.SMOKE_LABEL,
       COUNT(*)                    AS ALERT_WINDOWS,
       COUNT(DISTINCT a.H3_CELL)   AS HEXAGONS
FROM FACT_ALERT a
JOIN DIM_PIPELINE_CONFIG p ON p.RUN_ID = a.RUN_ID AND p.CONFIG_ID = a.CONFIG_ID
WHERE p.CONFIG_ID IN ('w10_wm5_q120_c60_r7', 'w10_q120_r7')
GROUP BY ALL
ORDER BY p.RUN_ID, p.CONFIG_ID, a.SMOKE_LABEL;

-- name: sensor_quality
-- Sensors whose readings fail QC most often (A/B channel disagreement or faults),
-- through the SCD Type 2 key that was valid at each reading's time.
SELECT d.SENSOR_INDEX, d.H3_CELL,
       COUNT(*)                                 AS READINGS,
       ROUND(AVG(IFF(r.PASSED_QC, 1, 0)), 3)    AS PASS_SHARE,
       ROUND(AVG(r.PM25_CORRECTED), 1)          AS MEAN_PM25_CORRECTED
FROM FACT_READING r
JOIN DIM_SENSOR d ON d.SENSOR_KEY = r.SENSOR_KEY
GROUP BY ALL
ORDER BY PASS_SHARE, READINGS DESC
LIMIT 10;

-- name: sensor_versions
-- The Type 2 history: one row per sensor version; sensors that moved have several.
SELECT COUNT(DISTINCT SENSOR_INDEX)                                    AS SENSORS,
       COUNT(*)                                                        AS VERSIONS,
       COUNT_IF(IS_CURRENT)                                            AS CURRENT_VERSIONS,
       COUNT(DISTINCT IFF(NOT IS_CURRENT, SENSOR_INDEX, NULL))         AS SENSORS_THAT_MOVED
FROM DIM_SENSOR;
