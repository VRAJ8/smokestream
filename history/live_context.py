#!/usr/bin/env python3
"""Live context for the streaming job: the latest fires, smoke plumes, wind and
monitor readings, in the formats the historical seasons use (common/context.py,
common/evaluate.py). The Airflow ingest DAGs run one command each on a schedule.

No API keys needed.

  fires     NASA FIRMS near-real-time VIIRS from NOAA-20 and NOAA-21, last 24 hours
            -> data/context/fires/fires_<date>.parquet
  plumes    NOAA HMS smoke polygons for yesterday and today (UTC) as H3 cells
            -> data/context/plume_cells/plumes_<date>.parquet
  wind      airport (ASOS) wind from the Iowa Environmental Mesonet, yesterday and today
            -> data/context/wind/wind_<date>.parquet
  monitors  AirNow hourly PM2.5 at regulatory monitors, last 3 hours
            -> data/monitors/monitors_<date>.parquet (ground truth; the pipelines never read it)

Files are per UTC day of the record. Each run merges what it fetched into those
files, and a record keeps the time it was first fetched as its published_at, so
the freshness rules in common/enrich.py treat live data as they treat the replays.
Monitor values keep the latest version instead, since AirNow revises recent hours.

The S-NPP near-real-time files (the satellite of the historical seasons) came
back empty in November 2026, so live fires use its two successors. These files
have no fire-type flag; low-confidence detections are dropped instead.

Usage:
  python history/live_context.py fires
  python history/live_context.py all --keep-days 14
"""

import argparse
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import pandas as pd  # noqa: E402

from history.download_context import RAW, REGIONS, asos_wind, expanded, fetch, plume_cells  # noqa: E402

CONTEXT = ROOT / "data" / "context"
MONITORS = ROOT / "data" / "monitors"

FIRMS_NRT = "https://firms.modaps.eosdis.nasa.gov/data/active_fire/{}/csv/{}_USA_contiguous_and_Hawaii_24h.csv"
SATELLITES = [("noaa-20-viirs-c2", "J1_VIIRS_C2"), ("noaa-21-viirs-c2", "J2_VIIRS_C2")]
AIRNOW = "https://files.airnowtech.org/airnow"


def merge_by_day(folder, prefix, frame, time_col, keys, keep="first"):
    """Merge rows into one Parquet file per UTC day of time_col; returns how many rows were new."""
    folder.mkdir(parents=True, exist_ok=True)
    day = pd.to_datetime(frame[time_col], unit="s", utc=True).dt.strftime("%Y-%m-%d")
    new = 0
    for tag, rows in frame.groupby(day):
        path = folder / f"{prefix}_{tag}.parquet"
        old = pd.read_parquet(path) if path.exists() else rows.iloc[:0]
        merged = pd.concat([old, rows], ignore_index=True).drop_duplicates(keys, keep=keep)
        new += len(merged) - len(old)
        tmp = path.with_suffix(".tmp")
        merged.to_parquet(tmp, index=False)
        tmp.replace(path)
    return new


def prune(folder, keep_days):
    """Delete day files older than keep_days."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=keep_days)).strftime("%Y-%m-%d")
    for path in folder.glob("*_????-??-??.parquet"):
        if path.stem[-10:] < cutoff:
            path.unlink()


# ---------- parsers (pure, so tests can feed them sample files) ----------

def parse_firms_nrt(path, bbox, published_at):
    west, south, east, north = bbox
    df = pd.read_csv(path, usecols=["latitude", "longitude", "acq_date", "acq_time", "confidence", "frp"],
                     dtype={"acq_time": str, "confidence": str})
    df = df[df["longitude"].between(west, east) & df["latitude"].between(south, north)
            & (df["confidence"].str.lower() != "low")]
    stamp = pd.to_datetime(df["acq_date"] + " " + df["acq_time"].str.zfill(4), format="%Y-%m-%d %H%M", utc=True)
    return pd.DataFrame({"detected_at": (stamp.astype("int64") // 10**9).astype(float),
                         "latitude": df["latitude"].astype(float), "longitude": df["longitude"].astype(float),
                         "frp_mw": df["frp"].astype(float), "published_at": float(published_at)})


def parse_airnow_sites(path, bbox):
    west, south, east, north = bbox
    df = pd.read_csv(path, sep="|", dtype=str)
    df = df[df["Parameter"] == "PM2.5"]
    lat, lon = df["Latitude"].astype(float), df["Longitude"].astype(float)
    df = df.assign(latitude=lat, longitude=lon)[lon.between(west, east) & lat.between(south, north)]
    return df.drop_duplicates("AQSID").set_index("AQSID")[["latitude", "longitude"]]


def parse_airnow_hour(path, sites):
    cols = ["date", "time", "site_id", "name", "gmt_offset", "parameter", "units", "pm25", "source"]
    df = pd.read_csv(path, sep="|", names=cols, dtype={"site_id": str}, encoding="latin-1")
    df = df[(df["parameter"] == "PM2.5") & df["site_id"].isin(sites.index)]
    hour = pd.to_datetime(df["date"] + " " + df["time"], format="%m/%d/%y %H:%M", utc=True)
    out = df.assign(hour_start=hour.astype("int64") // 10**9).join(sites, on="site_id")
    out["pm25"] = out["pm25"].astype(float).clip(lower=0)
    return out[["site_id", "latitude", "longitude", "hour_start", "pm25"]]


# ---------- commands ----------

def cmd_fires(region, now):
    frames = []
    for folder, name in SATELLITES:
        path = fetch(FIRMS_NRT.format(folder, name), RAW / "firms_nrt" / f"{name}_24h.csv", refresh=True)
        frames.append(parse_firms_nrt(path, expanded(region), now))
    fires = pd.concat(frames, ignore_index=True)
    new = merge_by_day(CONTEXT / "fires", "fires", fires, "detected_at", ["detected_at", "latitude", "longitude"])
    print(f"fires: {len(fires):,} detections in the last 24 h, {new:,} new -> {CONTEXT / 'fires'}")


def cmd_plumes(region, now):
    today = datetime.fromtimestamp(now, timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    cells, missing = plume_cells([today - timedelta(days=1), today], REGIONS[region], refresh=True)
    cells["published_at"] = float(now)
    keys = ["h3", "valid_start", "valid_end", "density"]  # a redrawn plume is a new record
    new = merge_by_day(CONTEXT / "plume_cells", "plumes", cells, "valid_start", keys)
    print(f"plumes: {len(cells):,} cell-days, {new:,} new" + (" (no HMS file for today yet)" if missing else "")
          + f" -> {CONTEXT / 'plume_cells'}")


def cmd_wind(region, now):
    today = datetime.fromtimestamp(now, timezone.utc)
    wind = asos_wind(today - timedelta(days=1), today + timedelta(days=1), region, RAW / "iem" / "asos_live.csv",
                     refresh=True)
    wind["published_at"] = float(now)
    new = merge_by_day(CONTEXT / "wind", "wind", wind, "observed_at", ["station_id", "observed_at"])
    print(f"wind: {wind['station_id'].nunique()} stations, {new:,} new observations -> {CONTEXT / 'wind'}")


def cmd_monitors(region, now):
    hour = datetime.fromtimestamp(now, timezone.utc).replace(minute=0, second=0, microsecond=0)
    sites_path = fetch(f"{AIRNOW}/today/Monitoring_Site_Locations_V2.dat",
                       RAW / "airnow" / f"sites_{hour:%Y%m%d}.dat")
    sites = parse_airnow_sites(sites_path, REGIONS[region])
    frames = []
    for h in (hour - timedelta(hours=i) for i in range(3)):
        try:
            path = fetch(f"{AIRNOW}/{h:%Y}/{h:%Y%m%d}/HourlyData_{h:%Y%m%d%H}.dat",
                         RAW / "airnow" / f"HourlyData_{h:%Y%m%d%H}.dat", refresh=True)
        except Exception:
            continue  # the latest hour is posted a little after the hour
        frames.append(parse_airnow_hour(path, sites))
    if not frames:
        sys.exit("monitors: no AirNow hourly file for the last 3 hours")
    mon = pd.concat(frames, ignore_index=True)
    merge_by_day(MONITORS, "monitors", mon, "hour_start", ["site_id", "hour_start"], keep="last")
    print(f"monitors: {mon['site_id'].nunique()} sites, {len(mon):,} site-hours -> {MONITORS}")


COMMANDS = {"fires": cmd_fires, "plumes": cmd_plumes, "wind": cmd_wind, "monitors": cmd_monitors}
FOLDERS = [CONTEXT / "fires", CONTEXT / "plume_cells", CONTEXT / "wind", MONITORS]


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("what", choices=[*COMMANDS, "all"])
    parser.add_argument("--region", choices=sorted(REGIONS), default="bayarea")
    parser.add_argument("--keep-days", type=int, default=14)
    args = parser.parse_args()
    now = time.time()
    for name in (COMMANDS if args.what == "all" else [args.what]):
        COMMANDS[name](args.region, now)
    for folder in FOLDERS:
        prune(folder, args.keep_days)


if __name__ == "__main__":
    main()
