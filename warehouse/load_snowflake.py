#!/usr/bin/env python3
"""Load SmokeStream data into the Snowflake warehouse (sql/snowflake_schema.sql).

  check    connect and show account, user, role and warehouse
  setup    create the database, schema and tables (safe to rerun)
  load     load everything found under data/; each part first deletes what it
           loaded last time, so reruns never duplicate rows
  verify   recompute the sweep's stream-vs-hindsight RMSE and label agreement in
           SQL and compare with data/out/season_*/sweep.csv (Python's numbers)
  query    run a named query from sql/warehouse_queries.sql (no name: list them)

What load puts where:
  DIM_TIME, FACT_MONITOR_HOURLY, FACT_FIRE_DETECTION,   per season, from data/history/<season>/
  FACT_PLUME_CELL, FACT_WIND_OBS
  DIM_SENSOR (SCD Type 2), FACT_READING                  each season's sensors.csv and history_avg10.csv
  DIM_PIPELINE_CONFIG, FACT_HEX_WINDOW, FACT_ALERT       per run: data/out/season_*/{stream,batch}_*/
                                                         and data/out/demo_live/stream
  DIM_H3_CELL                                            every hexagon in the tables above

Connection settings come from .env: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER and
SNOWFLAKE_PRIVATE_KEY_PATH (key-pair sign-in; Snowflake is retiring password-only
sign-in for scripts), optionally SNOWFLAKE_ROLE and SNOWFLAKE_WAREHOUSE.

Usage:
  python warehouse/load_snowflake.py check
  python warehouse/load_snowflake.py setup
  python warehouse/load_snowflake.py load            # --dry-run: build everything, send nothing
  python warehouse/load_snowflake.py verify
  python warehouse/load_snowflake.py query season_summary
"""

import argparse
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import h3  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from common import airquality as aq  # noqa: E402
from common.purpleair import load_env  # noqa: E402

DATABASE, SCHEMA = "SMOKESTREAM", "CORE"
H3_RES = 7
SCHEMA_SQL = ROOT / "sql" / "snowflake_schema.sql"
QUERIES_SQL = ROOT / "sql" / "warehouse_queries.sql"
HEX_COLUMNS = ["RUN_ID", "CONFIG_ID", "H3_CELL", "WINDOW_START", "WINDOW_END", "PM25", "PM1", "AQI", "AQI_CATEGORY",
               "N_READINGS", "N_SENSORS", "N_EXCLUDED", "EXTENT", "FINE_RATIO", "IN_PLUME", "PLUME_DENSITY",
               "WIND_DIR_DEG", "WIND_SPEED_MS", "UPWIND_FIRE", "NEAREST_FIRE_KM", "N_FIRES", "SMOKE_LABEL",
               "SMOKE_ALERT", "LAST_ARRIVAL_TIME", "EMITTED_SIM_TIME", "DECISION_DELAY_S", "EMITTED_AT"]


def connect():
    load_env(ROOT)
    need = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PRIVATE_KEY_PATH"]
    missing = [k for k in need if not os.environ.get(k)]
    if missing:
        sys.exit(f"Set {', '.join(missing)} in .env first (see warehouse/README.md).")
    key = Path(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]).expanduser()
    key = key if key.is_absolute() else ROOT / key
    import snowflake.connector
    options = {"account": os.environ["SNOWFLAKE_ACCOUNT"], "user": os.environ["SNOWFLAKE_USER"],
               "authenticator": "SNOWFLAKE_JWT", "private_key_file": str(key),
               "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE") or "COMPUTE_WH",
               "session_parameters": {"TIMEZONE": "UTC"}}
    if os.environ.get("SNOWFLAKE_ROLE"):
        options["role"] = os.environ["SNOWFLAKE_ROLE"]
    return snowflake.connector.connect(**options)


class Warehouse:
    def __init__(self, conn):
        self.conn = conn

    def run(self, sql, params=None):
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.rowcount

    def write(self, frame, table, temporary=False):
        from snowflake.connector.pandas_tools import write_pandas
        if temporary:
            write_pandas(self.conn, frame, table, database=DATABASE, schema=SCHEMA, use_logical_type=True,
                         auto_create_table=True, table_type="temporary", overwrite=True)
        elif len(frame):
            write_pandas(self.conn, frame, table, database=DATABASE, schema=SCHEMA, use_logical_type=True,
                         chunk_size=250_000)


class DryRun:
    """Stands in for the warehouse: everything is built and kept here, nothing is sent."""

    def __init__(self):
        self.tables = {}

    def run(self, sql, params=None):
        return 0

    def write(self, frame, table, temporary=False):
        self.tables[table] = frame if temporary or table not in self.tables else pd.concat([self.tables[table], frame])


def replace(db, table, frame, where, params):
    """Delete the slice of `table` this load owns, then append `frame`."""
    removed = db.run(f"DELETE FROM {table} WHERE {where}", params)
    db.write(frame, table)
    print(f"  {table}: {len(frame):,} rows" + (f" (replaced {removed:,})" if removed else ""))


def ts(seconds):
    return pd.to_datetime(seconds, unit="s").astype("datetime64[us]")


def cells(lat, lon):
    return [h3.latlng_to_cell(a, b, H3_RES) for a, b in zip(lat, lon)]


def between(frame, column):
    return {"lo": frame[column].min().to_pydatetime(), "hi": frame[column].max().to_pydatetime()}


# ---------- per season ----------

def load_time(db, season, monitors):
    start = pd.Timestamp(monitors["hour_start"].min(), unit="s").floor("D")
    end = pd.Timestamp(monitors["hour_start"].max() + 3600, unit="s").ceil("D")
    n = int((end - start).total_seconds() // 120)
    db.run("DELETE FROM DIM_TIME WHERE FIRE_SEASON = %(season)s", {"season": season})
    db.run("""
        INSERT INTO DIM_TIME (TIME_KEY, DATE_UTC, HOUR_UTC, DATE_LOCAL, HOUR_LOCAL, DAY_OF_WEEK, FIRE_SEASON)
        SELECT TS, TS::DATE, HOUR(TS), LOCAL_TS::DATE, HOUR(LOCAL_TS), DAYNAME(LOCAL_TS), %(season)s
        FROM (SELECT TS, CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TS) AS LOCAL_TS
              FROM (SELECT DATEADD(MINUTE, 2 * (ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1),
                                   %(start)s::TIMESTAMP_NTZ) AS TS
                    FROM TABLE(GENERATOR(ROWCOUNT => %(n)s))))""",
           {"season": season, "start": start.to_pydatetime(), "n": n})
    print(f"  DIM_TIME: {n:,} two-minute slots, {start.date()} to {end.date()}")


def load_monitors(db, monitors):
    pm = monitors["pm25"]
    frame = pd.DataFrame({
        "SITE_ID": monitors["site_id"].astype(str), "LATITUDE": monitors["latitude"],
        "LONGITUDE": monitors["longitude"], "H3_CELL": cells(monitors["latitude"], monitors["longitude"]),
        "HOUR_START_UTC": ts(monitors["hour_start"]), "PM25": pm,
        "AQI": [aq.pm25_to_aqi(v) if pd.notna(v) else None for v in pm]})
    replace(db, "FACT_MONITOR_HOURLY", frame, "HOUR_START_UTC BETWEEN %(lo)s AND %(hi)s", between(frame, "HOUR_START_UTC"))


def load_context(db, context):
    fires = pd.read_parquet(context / "fires")
    frame = pd.DataFrame({
        "DETECTED_AT": ts(fires["detected_at"]), "PUBLISHED_AT": ts(fires["published_at"]),
        "LATITUDE": fires["latitude"], "LONGITUDE": fires["longitude"],
        "H3_CELL": cells(fires["latitude"], fires["longitude"]), "FRP_MW": fires["frp_mw"]})
    replace(db, "FACT_FIRE_DETECTION", frame, "DETECTED_AT BETWEEN %(lo)s AND %(hi)s", between(frame, "DETECTED_AT"))

    plumes = pd.read_parquet(context / "plume_cells")
    frame = pd.DataFrame({
        "H3_CELL": plumes["h3"], "VALID_START": ts(plumes["valid_start"]), "VALID_END": ts(plumes["valid_end"]),
        "DENSITY": plumes["density"], "PUBLISHED_AT": ts(plumes["published_at"])})
    replace(db, "FACT_PLUME_CELL", frame, "VALID_START BETWEEN %(lo)s AND %(hi)s", between(frame, "VALID_START"))

    wind = pd.read_parquet(context / "wind")
    frame = pd.DataFrame({
        "STATION_ID": wind["station_id"], "LATITUDE": wind["latitude"], "LONGITUDE": wind["longitude"],
        "OBSERVED_AT": ts(wind["observed_at"]), "PUBLISHED_AT": ts(wind["published_at"]),
        "WIND_DIR_DEG": wind["wind_dir_deg"], "WIND_SPEED_MS": wind["wind_speed_ms"]})
    replace(db, "FACT_WIND_OBS", frame, "OBSERVED_AT BETWEEN %(lo)s AND %(hi)s", between(frame, "OBSERVED_AT"))


def load_sensors(db, path):
    """Apply one sensor snapshot to DIM_SENSOR as a Type 2 change.

    A sensor whose location changed gets its current row closed at the snapshot
    time and a new current row. A sensor seen for the first time is valid from its
    creation date. Apply snapshots in time order; reapplying one changes nothing.
    """
    snap = pd.read_csv(path)
    as_of = pd.Timestamp(path.stat().st_mtime, unit="s").floor("s").to_pydatetime()  # when it was observed
    stage = pd.DataFrame({
        "SENSOR_INDEX": snap["sensor_index"], "LATITUDE": snap["latitude"], "LONGITUDE": snap["longitude"],
        "H3_CELL": cells(snap["latitude"], snap["longitude"]), "CREATED": ts(snap["date_created"])})
    db.write(stage, "STG_SENSOR", temporary=True)
    closed = db.run("""
        UPDATE DIM_SENSOR d SET VALID_TO = %(as_of)s, IS_CURRENT = FALSE
        FROM STG_SENSOR s
        WHERE d.SENSOR_INDEX = s.SENSOR_INDEX AND d.IS_CURRENT AND d.VALID_FROM < %(as_of)s
          AND (d.LATITUDE <> s.LATITUDE OR d.LONGITUDE <> s.LONGITUDE)""", {"as_of": as_of})
    added = db.run("""
        INSERT INTO DIM_SENSOR (SENSOR_INDEX, LATITUDE, LONGITUDE, H3_CELL, VALID_FROM, VALID_TO, IS_CURRENT)
        SELECT s.SENSOR_INDEX, s.LATITUDE, s.LONGITUDE, s.H3_CELL,
               IFF(p.SENSOR_INDEX IS NULL, s.CREATED, %(as_of)s::TIMESTAMP_NTZ), NULL, TRUE
        FROM STG_SENSOR s
        LEFT JOIN (SELECT DISTINCT SENSOR_INDEX FROM DIM_SENSOR) p ON p.SENSOR_INDEX = s.SENSOR_INDEX
        WHERE NOT EXISTS (SELECT 1 FROM DIM_SENSOR d WHERE d.SENSOR_INDEX = s.SENSOR_INDEX AND d.IS_CURRENT)""",
                   {"as_of": as_of})
    print(f"  DIM_SENSOR: snapshot of {len(stage)} sensors; {added} new versions, {closed} closed")


def load_readings(db, path):
    raw = pd.read_csv(path)
    a, b, rh = raw["pm2.5_cf_1_a"], raw["pm2.5_cf_1_b"], raw["humidity_a"]
    passed = aq.valid_ab_mask(a, b, rh)
    pa = (a + b) / 2
    stage = pd.DataFrame({
        "SENSOR_INDEX": raw["sensor_index"], "EVENT_TS": ts(raw["time_stamp"]),
        "TIME_KEY": ts(raw["time_stamp"] // 120 * 120), "PM25_A": a, "PM25_B": b, "PM1": raw["pm1.0_cf_1_a"],
        "RH": rh, "PASSED_QC": passed,
        "PM25_CORRECTED": [aq.epa_correct(p, r) if ok else None for p, r, ok in zip(pa, rh, passed)]})
    db.write(stage, "STG_READING", temporary=True)
    params = between(stage, "EVENT_TS")
    removed = db.run("DELETE FROM FACT_READING WHERE EVENT_TS BETWEEN %(lo)s AND %(hi)s", params)
    added = db.run("""
        INSERT INTO FACT_READING (SENSOR_KEY, TIME_KEY, EVENT_TS, PM25_A, PM25_B, PM1, RH, PM25_CORRECTED, PASSED_QC)
        SELECT d.SENSOR_KEY, r.TIME_KEY, r.EVENT_TS, r.PM25_A, r.PM25_B, r.PM1, r.RH, r.PM25_CORRECTED, r.PASSED_QC
        FROM STG_READING r
        JOIN DIM_SENSOR d ON d.SENSOR_INDEX = r.SENSOR_INDEX
         AND r.EVENT_TS >= d.VALID_FROM AND (d.VALID_TO IS NULL OR r.EVENT_TS < d.VALID_TO)""")
    print(f"  FACT_READING: {len(stage):,} readings ({passed.mean():.1%} pass QC); {added:,} matched a sensor version"
          + (f" (replaced {removed:,})" if removed else ""))


# ---------- per pipeline run ----------

def runs():
    found = sorted(p for p in (ROOT / "data" / "out").glob("season_*/*_*") if p.is_dir()
                   and re.match(r"(stream|batch)_", p.name) and list(p.glob("config_id=*")))
    live = ROOT / "data" / "out" / "demo_live" / "stream"
    if list(live.glob("config_id=*")):
        found.append(live)
    return found


def run_id(path):
    return "demo_live" if path.parent.name == "demo_live" else path.name


def knobs(config_id):
    get = lambda key: int(m.group(1)) if (m := re.search(rf"(?:^|_){key}(\d+)(?:_|$)", config_id)) else None
    return {"WINDOW_MINUTES": get("w"), "WATERMARK_MINUTES": get("wm"), "QC_HISTORY_MINUTES": get("q"),
            "CONTEXT_REFRESH_MINUTES": get("c"), "H3_RESOLUTION": get("r")}


def load_run(db, path):
    rid = run_id(path)
    out = pd.read_parquet(path)
    out["config_id"] = out["config_id"].astype(str)
    if "emitted_at" in out:
        mode = "stream"
    elif "decision_delay_s" in out:
        mode = "simulated"
    else:
        mode = "batch"
    season = m.group(0) if (m := re.search(r"\d{4}", rid)) else str(out["window_start"].min().year)

    configs = pd.DataFrame([{"RUN_ID": rid, "CONFIG_ID": c, "MODE": mode, **knobs(c), "FIRE_SEASON": season}
                            for c in sorted(out["config_id"].unique())])
    knob_columns = [c for c in configs if c.endswith(("_MINUTES", "_RESOLUTION"))]
    configs[knob_columns] = configs[knob_columns].astype("Int64")  # batch configs have no watermark
    replace(db, "DIM_PIPELINE_CONFIG", configs, "RUN_ID = %(rid)s", {"rid": rid})

    frame = out.rename(columns={"h3": "H3_CELL"}).rename(columns=str.upper)
    frame["RUN_ID"] = rid
    for col in HEX_COLUMNS:
        if col not in frame:  # columns only some modes produce, e.g. EMITTED_AT for real streaming runs
            frame[col] = pd.NaT if col == "EMITTED_AT" else np.nan
    for col in ("WINDOW_START", "WINDOW_END", "EMITTED_AT"):
        frame[col] = pd.to_datetime(frame[col]).dt.tz_localize(None).astype("datetime64[us]")
    replace(db, "FACT_HEX_WINDOW", frame[HEX_COLUMNS], "RUN_ID = %(rid)s", {"rid": rid})

    db.run("DELETE FROM FACT_ALERT WHERE RUN_ID = %(rid)s", {"rid": rid})
    alerts = db.run("""
        INSERT INTO FACT_ALERT (RUN_ID, CONFIG_ID, H3_CELL, WINDOW_START, AQI, AQI_CATEGORY, SMOKE_LABEL, ALERT_SIM_TIME)
        SELECT RUN_ID, CONFIG_ID, H3_CELL, WINDOW_START, AQI, AQI_CATEGORY, SMOKE_LABEL,
               TO_TIMESTAMP_NTZ(EMITTED_SIM_TIME::NUMBER)
        FROM FACT_HEX_WINDOW WHERE RUN_ID = %(rid)s AND SMOKE_ALERT""", {"rid": rid})
    print(f"  FACT_ALERT: {alerts:,} alert windows")


def load_cells(db):
    added = db.run("""
        MERGE INTO DIM_H3_CELL d
        USING (SELECT H3_CELL FROM FACT_HEX_WINDOW UNION SELECT H3_CELL FROM FACT_PLUME_CELL
               UNION SELECT H3_CELL FROM FACT_MONITOR_HOURLY UNION SELECT H3_CELL FROM FACT_FIRE_DETECTION
               UNION SELECT H3_CELL FROM DIM_SENSOR) s
        ON d.H3_CELL = s.H3_CELL
        WHEN NOT MATCHED THEN INSERT (H3_CELL, RESOLUTION, CENTER)
             VALUES (s.H3_CELL, H3_GET_RESOLUTION(s.H3_CELL), H3_CELL_TO_POINT(s.H3_CELL))""")
    print(f"  DIM_H3_CELL: {added:,} new hexagons")


# ---------- commands ----------

def cmd_check(db):
    with db.conn.cursor() as cur:
        account, user, role, warehouse, region = cur.execute(
            "SELECT CURRENT_ACCOUNT(), CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_REGION()").fetchone()
    print(f"Connected to Snowflake: account {account} ({region}), user {user}, role {role}, warehouse {warehouse}")
    if not warehouse:
        print(f"Warehouse {os.environ.get('SNOWFLAKE_WAREHOUSE') or 'COMPUTE_WH'} is missing or not granted to "
              "this role; set SNOWFLAKE_WAREHOUSE in .env to one you can use.")


def cmd_setup(db):
    for cur in db.conn.execute_string(SCHEMA_SQL.read_text()):
        cur.close()
    with db.conn.cursor() as cur:
        tables = [r[1] for r in cur.execute(f"SHOW TABLES IN SCHEMA {DATABASE}.{SCHEMA}").fetchall()]
    print(f"{DATABASE}.{SCHEMA} ready: {', '.join(sorted(tables))}")


def cmd_load(db):
    if isinstance(db, Warehouse):
        db.run(f"USE SCHEMA {DATABASE}.{SCHEMA}")
    for base in sorted((ROOT / "data" / "history").glob("[0-9]" * 4)):
        season = base.name
        print(f"Season {season}")
        monitors = pd.read_csv(base / "monitors" / "monitors.csv", dtype={"site_id": str})
        load_time(db, season, monitors)
        load_monitors(db, monitors)
        if (base / "context").exists():
            load_context(db, base / "context")
        if (base / "sensors.csv").exists():
            load_sensors(db, base / "sensors.csv")
        if (base / "history_avg10.csv").exists():
            load_readings(db, base / "history_avg10.csv")
    for window in sorted((ROOT / "data" / "history").glob("[0-9]" * 4 + "-*")):
        if (window / "history_avg10.csv").exists():
            print(f"Event window {window.name}")
            load_sensors(db, window / "sensors.csv")
            load_readings(db, window / "history_avg10.csv")
    for path in runs():
        print(f"Run {run_id(path)}")
        load_run(db, path)
    print("Hexagons")
    load_cells(db)


def queries():
    named, name = {}, None
    for line in QUERIES_SQL.read_text().splitlines():
        if m := re.match(r"--\s*name:\s*(\w+)", line):
            name = m.group(1)
            named[name] = []
        elif name:
            named[name].append(line)
    return {k: "\n".join(v).strip().rstrip(";") for k, v in named.items()}


def fetch(db, sql):
    with db.conn.cursor() as cur:
        cur.execute(f"USE SCHEMA {DATABASE}.{SCHEMA}")
        cur.execute(sql)
        frame = cur.fetch_pandas_all()
    return frame


def cmd_verify(db):
    sweeps = [pd.read_csv(p) for p in (ROOT / "data" / "out").glob("season_*/sweep.csv")]
    if not sweeps:
        sys.exit("No sweep results in data/out/season_*/sweep.csv to compare with.")
    py = pd.concat(sweeps)
    py["STREAM_RUN"] = "stream_" + py["season"].astype(str)
    sql = fetch(db, queries()["rq1_stream_vs_hindsight"])
    j = py.merge(sql, left_on=["STREAM_RUN", "stream_config"], right_on=["STREAM_RUN", "STREAM_CONFIG"], how="left")
    j["rmse_diff"] = (j["RMSE_UGM3"].astype(float) - j["rmse_ugm3"]).abs()
    j["label_diff"] = (j["LABEL_MATCH"].astype(float) - j["label_match"]).abs()
    j["windows_diff"] = (j["BASELINE_WINDOWS"] - j["baseline_windows"]).abs()
    print(j[["STREAM_RUN", "stream_config", "rmse_ugm3", "RMSE_UGM3", "label_match", "LABEL_MATCH",
             "baseline_windows", "BASELINE_WINDOWS"]].to_string(index=False))
    worst = j[["rmse_diff", "label_diff", "windows_diff"]].max()
    ok = j["RMSE_UGM3"].notna().all() and worst["rmse_diff"] < 1e-6 and worst["label_diff"] < 1e-6 and worst["windows_diff"] == 0
    print(f"\n{'MATCH' if ok else 'MISMATCH'}: {len(j)} settings; largest differences "
          f"RMSE {worst['rmse_diff']:.2g}, label agreement {worst['label_diff']:.2g}, windows {worst['windows_diff']:.0f}")
    if not ok:
        sys.exit(1)


def cmd_query(db, name):
    named = queries()
    if not name or name not in named:
        print("Queries in sql/warehouse_queries.sql:", ", ".join(named))
        return
    print(fetch(db, named[name]).to_string(index=False))


COMMANDS = {"check": cmd_check, "setup": cmd_setup, "load": cmd_load, "verify": cmd_verify}


def run(command, name=None):
    """Connect, run one command, disconnect. The Airflow DAG calls this too."""
    conn = connect()
    try:
        db = Warehouse(conn)
        cmd_query(db, name) if command == "query" else COMMANDS[command](db)
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=[*COMMANDS, "query"])
    parser.add_argument("name", nargs="?", help="query name, for the query command")
    parser.add_argument("--dry-run", action="store_true", help="load: build every table without connecting")
    args = parser.parse_args()
    if args.command == "load" and args.dry_run:
        cmd_load(DryRun())
    else:
        run(args.command, args.name)


if __name__ == "__main__":
    main()
