"""SmokeStream Airflow DAGs: everything that is not real-time.

Owner: Brandon. Each task is a stub with its contract (what it fetches, where it
writes, which columns) so the pieces fit the Spark jobs and the Snowflake schema
(sql/snowflake_schema.sql). Fill in the bodies; keep the output contracts.

Context lookups go to data/context/<name>/ as Parquet, in exactly the columns
common/context.py defines (all times UNIX seconds, UTC); the pipelines read them
with load_context(). published_at is when the data became available; leave it
null if the source does not say and enrich.py assumes a typical delay. Every DAG
starts paused, so nothing runs until you switch it on in the Airflow UI.

Works with Airflow 2.x (airflow.decorators) and 3.x (airflow.sdk).
"""

from datetime import datetime, timedelta
from pathlib import Path

try:  # Airflow 3
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2
    from airflow.decorators import dag, task

PROJECT = Path(__file__).resolve().parents[2]
CONTEXT = PROJECT / "data" / "context"
# Northern California: west, south, east, north (same box as the recorder)
BBOX = (-124.0, 36.5, -119.5, 41.0)

DEFAULTS = {"owner": "smokestream", "retries": 2, "retry_delay": timedelta(minutes=5)}
COMMON = {"start_date": datetime(2026, 10, 1), "catchup": False, "default_args": DEFAULTS,
          "is_paused_upon_creation": True, "tags": ["smokestream"]}


@dag(schedule=timedelta(minutes=30), **COMMON)
def ingest_fire_detections():
    @task
    def fetch_firms():
        """NASA FIRMS active fires (VIIRS SNPP, NOAA-20, MODIS) inside BBOX, last 24 h.

        Needs FIRMS_MAP_KEY. Area API: /api/area/csv/{MAP_KEY}/{SOURCE}/{w,s,e,n}/{days}
        (check the current docs at firms.modaps.eosdis.nasa.gov/api).
        Output: data/context/fires/fires_<utc timestamp>.parquet with columns
          detected_at, latitude, longitude, frp_mw, published_at
        (detected_at from the acquisition date and time; published_at = time of download)
        """
        raise NotImplementedError

    fetch_firms()


@dag(schedule=timedelta(hours=3), **COMMON)
def ingest_smoke_plumes():
    @task
    def fetch_hms():
        """NOAA Hazard Mapping System smoke polygons for today (UTC).

        HMS publishes daily shapefiles/KML of analyst-drawn plumes with a density
        class (Light, Medium, Heavy); see NOAA OSPO's HMS page for the file location.
        Convert each polygon to H3 cells with common.context.plume_to_cells(ring, 7).
        Output: data/context/plume_cells/plumes_<date>.parquet, one row per cell, columns
          h3, valid_start, valid_end, density, published_at
        """
        raise NotImplementedError

    fetch_hms()


@dag(schedule=timedelta(hours=1), **COMMON)
def ingest_weather():
    @task
    def fetch_wind():
        """Latest wind speed and direction from NWS stations in BBOX.

        api.weather.gov needs no key but requires a User-Agent header with contact info.
        Output: data/context/wind/wind_<utc timestamp>.parquet with columns
          station_id, latitude, longitude, observed_at, wind_dir_deg, wind_speed_ms, published_at
        (wind_dir_deg is where the wind blows FROM, as NWS reports it)
        """
        raise NotImplementedError

    fetch_wind()


@dag(schedule=timedelta(hours=1), **COMMON)
def ingest_monitors():
    @task
    def fetch_airnow():
        """Hourly PM2.5 from regulatory monitors in BBOX (EPA AirNow API; needs AIRNOW_API_KEY).

        Output: data/monitors/monitors_<utc hour>.parquet with columns
          site_id, latitude, longitude, hour_start (UNIX seconds, start of the hour, UTC), pm25
        Ground truth only: never put it under data/context/, which the pipelines read.
        common/evaluate.py reads this format. Loads into FACT_MONITOR_HOURLY.
        """
        raise NotImplementedError

    fetch_airnow()


@dag(schedule=None, **COMMON)
def backfill_fire_seasons():
    """Manual: historical monitors and context for the 2020, 2021 and 2024 seasons (run once).

    Each task runs one step of history/download_context.py, which downloads the
    public files (cached under data/raw/) and writes data/history/<season>/.
    """

    @task
    def backfill(season: str, what: str):
        import sys
        sys.path.insert(0, str(PROJECT))
        from history.download_context import COMMANDS
        COMMANDS[what](season, "bayarea")

    for season in ("2020", "2021", "2024"):
        for what in ("monitors", "fires", "plumes", "wind"):
            backfill.override(task_id=f"{what}_{season}")(season, what)


@dag(schedule=timedelta(minutes=5), **COMMON)
def load_snowflake():
    @task
    def copy_new_outputs():
        """PUT new Parquet files from data/out/{stream,batch} to a Snowflake stage and
        COPY INTO FACT_HEX_WINDOW (columns match the Spark output; config_id comes from
        the partition folder name). Track loaded files so nothing loads twice.
        Needs SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER and a password or key-pair (Airflow connection)."""
        raise NotImplementedError

    copy_new_outputs()


@dag(schedule="0 9 * * *", **COMMON)  # 09:00 UTC = 1-2 a.m. in California
def nightly_sensor_scores():
    @task
    def score_sensors():
        """Reliability score per sensor from the last 7 days: share of readings passing
        the A/B check, uptime, and agreement with neighbouring sensors in the same hexagon.
        Writes data/context/sensor_scores.parquet (sensor_index, reliability_score) for the
        stream, and updates DIM_SENSOR as a Type 2 change when a score or location changes."""
        raise NotImplementedError

    score_sensors()


ingest_fire_detections()
ingest_smoke_plumes()
ingest_weather()
ingest_monitors()
backfill_fire_seasons()
load_snowflake()
nightly_sensor_scores()
