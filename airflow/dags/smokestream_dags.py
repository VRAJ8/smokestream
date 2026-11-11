"""SmokeStream Airflow DAGs: everything that is not real-time.

Each task runs one project script with the project's own Python (.venv), so
Airflow lives in its own environment (.venv-airflow) and never shares packages
with Spark. Start it with airflow/run_airflow.sh.

  ingest_*               live context into data/context/ (history/live_context.py), where the
                         streaming job reads it; monitor readings go to data/monitors/ for
                         evaluation only
  backfill_fire_seasons  historical monitors and context for 2020, 2021 and 2024 (run once)
  load_snowflake         warehouse tables, data, and a check against the Python results

How often the ingest DAGs run is the live counterpart of the context-refresh knob
in the experiments. Every DAG starts paused, so nothing runs until it is switched
on in the Airflow UI.

Works with Airflow 2.9+ (airflow.decorators) and 3.x (airflow.sdk).
"""

from datetime import datetime, timedelta
from pathlib import Path

try:  # Airflow 3
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2
    from airflow.decorators import dag, task

PROJECT = Path(__file__).resolve().parents[2]
PY = PROJECT / ".venv" / "bin" / "python"

DEFAULTS = {"owner": "smokestream", "retries": 2, "retry_delay": timedelta(minutes=5)}
COMMON = {"start_date": datetime(2026, 10, 1), "catchup": False, "default_args": DEFAULTS,
          "is_paused_upon_creation": True, "tags": ["smokestream"]}


def script(*args):
    """Shell command that runs a project script with the project's Python."""
    return f"cd '{PROJECT}' && '{PY}' " + " ".join(map(str, args))


@dag(schedule=timedelta(minutes=30), **COMMON)
def ingest_fire_detections():
    """NASA FIRMS near-real-time VIIRS fire detections, last 24 hours."""
    @task.bash
    def fetch_firms():
        return script("history/live_context.py", "fires")

    fetch_firms()


@dag(schedule=timedelta(hours=3), **COMMON)
def ingest_smoke_plumes():
    """NOAA HMS smoke polygons for yesterday and today, as H3 cells."""
    @task.bash
    def fetch_hms():
        return script("history/live_context.py", "plumes")

    fetch_hms()


@dag(schedule=timedelta(hours=1), **COMMON)
def ingest_weather():
    """Airport (ASOS) wind speed and direction around the study region."""
    @task.bash
    def fetch_wind():
        return script("history/live_context.py", "wind")

    fetch_wind()


@dag(schedule="25 * * * *", **COMMON)  # AirNow posts each hour a little after it ends
def ingest_monitors():
    """AirNow hourly PM2.5 at regulatory monitors: ground truth, never read by the pipelines."""
    @task.bash
    def fetch_airnow():
        return script("history/live_context.py", "monitors")

    fetch_airnow()


@dag(schedule=None, **COMMON)
def backfill_fire_seasons():
    """Manual: historical monitors and context for the 2020, 2021 and 2024 seasons (run once).

    Each task runs one step of history/download_context.py, which downloads the
    public files (cached under data/raw/) and writes data/history/<season>/.
    """
    for season in ("2020", "2021", "2024"):
        for what in ("monitors", "fires", "plumes", "wind"):
            task.bash(task_id=f"{what}_{season}")(lambda w=what, s=season: script(
                "history/download_context.py", w, "--season", s))()


@dag(schedule="0 10 * * *", **COMMON)
def load_snowflake():
    """Warehouse load with warehouse/load_snowflake.py: tables, then data, then a check
    that Snowflake's RQ1 numbers match the Python sweep (the task fails if they do not).
    Each load replaces the slice it loaded before, so reruns never duplicate rows.
    Credentials come from the project's .env (key-pair sign-in)."""
    @task.bash
    def create_tables():
        return script("warehouse/load_snowflake.py", "setup")

    @task.bash
    def load_tables():
        return script("warehouse/load_snowflake.py", "load")

    @task.bash
    def verify_against_python():
        return script("warehouse/load_snowflake.py", "verify")

    create_tables() >> load_tables() >> verify_against_python()


ingest_fire_detections()
ingest_smoke_plumes()
ingest_weather()
ingest_monitors()
backfill_fire_seasons()
load_snowflake()
