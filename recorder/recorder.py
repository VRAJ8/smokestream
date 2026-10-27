#!/usr/bin/env python3
"""Live PurpleAir recorder for the SmokeStream delay profile.

Polls a fixed random sample of outdoor PurpleAir sensors in Northern California
and appends one row per sensor per poll. Each row keeps both the sensor's
last_seen time (when PurpleAir last received data from it) and our poll time,
so we can measure how stale each reading is when a real-time system would use
it, and how often sensors drop out. The replay tool later injects this same
delay profile into the historical fire seasons.

Commands:
  python3 recorder/recorder.py sample   pick the sensor sample (run once)
  python3 recorder/recorder.py once     one poll, to check the points cost
  python3 recorder/recorder.py run      poll until stopped (Ctrl+C)
  python3 recorder/recorder.py status   summary of what has been recorded

Only the Python standard library is used, so no install step is needed.
"""

import csv
import json
import os
import random
import statistics
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data" / "live"
SAMPLE_FILE = DATA_DIR / "sensors_sample.csv"
POLLS_FILE = DATA_DIR / "polls.csv"
API_URL = "https://api.purpleair.com/v1/sensors"

# "minimal" is enough for the delay and dropout profile and keeps the points
# cost low. "full" also records the values needed for the EPA correction.
FIELD_PROFILES = {
    "minimal": ["last_seen"],
    "full": ["last_seen", "pm2.5_cf_1_a", "pm2.5_cf_1_b", "pm1.0_cf_1_a", "humidity_a"],
}

POLL_COLUMNS = ["poll_epoch", "poll_time_utc", "status", "rows", "request_ms", "data_time_stamp", "error"]


def load_env():
    """Read KEY=VALUE pairs from the project's .env file into os.environ."""
    env_path = ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def setting(name, default):
    value = os.environ.get(name, "").strip()
    return type(default)(value) if value else default


def api_key():
    key = os.environ.get("PURPLEAIR_READ_KEY", "").strip()
    if not key:
        sys.exit("PURPLEAIR_READ_KEY is missing. Copy .env.example to .env and paste your READ key into it.")
    return key


def utc_iso(epoch):
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def fetch(params, retries=5):
    """GET /v1/sensors. Retries on network errors, rate limits and server errors."""
    url = API_URL + "?" + urllib.parse.urlencode(params)
    request = urllib.request.Request(url, headers={"X-API-Key": api_key()})
    delay = 5
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as err:
            body = err.read().decode("utf-8", errors="replace")[:300]
            if err.code == 429 or err.code >= 500:
                if attempt == retries:
                    raise RuntimeError(f"HTTP {err.code} after {retries} attempts: {body}")
            else:
                # Bad key, no points left, bad parameters: retrying will not help.
                raise RuntimeError(f"HTTP {err.code}: {body}")
        except (urllib.error.URLError, TimeoutError, ConnectionError) as err:
            if attempt == retries:
                raise RuntimeError(f"network error after {retries} attempts: {err}")
        time.sleep(delay)
        delay = min(delay * 2, 60)


def rows_as_dicts(payload):
    fields = payload.get("fields", [])
    return [dict(zip(fields, row)) for row in payload.get("data", [])]


def read_sample():
    if not SAMPLE_FILE.exists():
        sys.exit("No sensor sample yet. Run: python3 recorder/recorder.py sample")
    with SAMPLE_FILE.open() as f:
        return [int(row["sensor_index"]) for row in csv.DictReader(f)]


def cmd_sample():
    """Pick a reproducible random sample of active outdoor sensors in the bounding box."""
    size = setting("SAMPLE_SIZE", 200)
    seed = setting("SAMPLE_SEED", 226)
    params = {
        "fields": "latitude,longitude,last_seen",
        "location_type": 0,   # outdoor sensors only
        "max_age": 3600,      # heard from in the last hour
        "nwlat": setting("BBOX_NW_LAT", 41.0),
        "nwlng": setting("BBOX_NW_LNG", -124.0),
        "selat": setting("BBOX_SE_LAT", 36.5),
        "selng": setting("BBOX_SE_LNG", -119.5),
    }
    sensors = rows_as_dicts(fetch(params))
    if not sensors:
        sys.exit("The API returned no sensors for this bounding box.")
    random.Random(seed).shuffle(sensors)
    chosen = sorted(sensors[:size], key=lambda s: s["sensor_index"])

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    sampled_at = utc_iso(time.time())
    with SAMPLE_FILE.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["sensor_index", "latitude", "longitude", "sampled_at_utc"])
        for s in chosen:
            writer.writerow([s["sensor_index"], round(s["latitude"], 4), round(s["longitude"], 4), sampled_at])
    print(f"{len(sensors)} active outdoor sensors in the box; sampled {len(chosen)} (seed {seed}).")
    print(f"Saved to {SAMPLE_FILE.relative_to(ROOT)}")


def append_csv(path, columns, rows):
    new_file = not path.exists()
    with path.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        if new_file:
            writer.writeheader()
        writer.writerows(rows)


def poll_once(sensor_ids, fields, profile):
    """One poll. Returns the number of rows written; logs the poll either way."""
    poll_epoch = time.time()
    log = {"poll_epoch": round(poll_epoch, 3), "poll_time_utc": utc_iso(poll_epoch)}
    started = time.monotonic()
    try:
        payload = fetch({"fields": ",".join(fields), "show_only": ",".join(map(str, sensor_ids))})
    except RuntimeError as err:
        log.update(status="error", rows=0, request_ms=round((time.monotonic() - started) * 1000), error=str(err))
        append_csv(POLLS_FILE, POLL_COLUMNS, [log])
        print(f"{log['poll_time_utc']}  error: {err}", flush=True)
        return 0

    request_ms = round((time.monotonic() - started) * 1000)
    data_ts = payload.get("data_time_stamp")
    rows = []
    for r in rows_as_dicts(payload):
        row = {"poll_epoch": log["poll_epoch"], "poll_time_utc": log["poll_time_utc"], "data_time_stamp": data_ts}
        row.update(r)
        last_seen = r.get("last_seen")
        row["staleness_s"] = round(poll_epoch - last_seen, 1) if last_seen else ""
        rows.append(row)

    day = log["poll_time_utc"][:10]
    columns = ["poll_epoch", "poll_time_utc", "data_time_stamp", "sensor_index", *fields, "staleness_s"]
    # One file per field profile and day, so switching profiles never mixes column sets.
    append_csv(DATA_DIR / f"readings_{profile}_{day}.csv", columns, rows)
    log.update(status="ok", rows=len(rows), request_ms=request_ms, data_time_stamp=data_ts, error="")
    append_csv(POLLS_FILE, POLL_COLUMNS, [log])
    print(f"{log['poll_time_utc']}  ok  {len(rows)} rows  {request_ms} ms", flush=True)
    return len(rows)


def field_profile():
    name = setting("FIELD_PROFILE", "minimal")
    if name not in FIELD_PROFILES:
        sys.exit(f"FIELD_PROFILE must be one of {', '.join(FIELD_PROFILES)}")
    return name, FIELD_PROFILES[name]


def cmd_once():
    sensor_ids, (profile, fields) = read_sample(), field_profile()
    rows = poll_once(sensor_ids, fields, profile)
    interval = setting("POLL_SECONDS", 300)
    polls_per_week = 7 * 86400 // interval
    print()
    print(f"Requested {len(sensor_ids)} sensors x {len(fields)} field(s); got {rows} rows.")
    print("Now open develop.purpleair.com and note how many points this one call used.")
    print(f"A week at POLL_SECONDS={interval} is {polls_per_week} calls, so the week costs")
    print(f"about (points for this call) x {polls_per_week}. New accounts start with 1,000,000 points.")


def cmd_run():
    sensor_ids, (profile, fields) = read_sample(), field_profile()
    interval = setting("POLL_SECONDS", 300)
    print(f"Recording {len(sensor_ids)} sensors every {interval}s with fields {fields}. Ctrl+C to stop.", flush=True)
    next_poll = time.time()
    try:
        while True:
            poll_once(sensor_ids, fields, profile)
            next_poll += interval
            now = time.time()
            if now > next_poll:
                # We fell behind, usually because the laptop slept. The gap shows up
                # in polls.csv so the analysis can exclude it; just resume the schedule.
                next_poll = now + interval
            time.sleep(max(0, next_poll - time.time()))
    except KeyboardInterrupt:
        print("\nStopped.")


def cmd_status():
    if not POLLS_FILE.exists():
        sys.exit("Nothing recorded yet.")
    with POLLS_FILE.open() as f:
        polls = list(csv.DictReader(f))
    ok = [p for p in polls if p["status"] == "ok"]
    epochs = [float(p["poll_epoch"]) for p in polls]
    gaps = [b - a for a, b in zip(epochs, epochs[1:])]
    interval = setting("POLL_SECONDS", 300)
    hours = (epochs[-1] - epochs[0]) / 3600 if len(epochs) > 1 else 0

    print(f"Polls: {len(polls)} ({len(ok)} ok, {len(polls) - len(ok)} errors)")
    print(f"First: {polls[0]['poll_time_utc']}   Last: {polls[-1]['poll_time_utc']}   Span: {hours:.1f} h of 168 h")
    if gaps:
        long_gaps = [g for g in gaps if g > 2 * interval]
        print(f"Longest gap between polls: {max(gaps) / 60:.1f} min; gaps over {2 * interval // 60} min: {len(long_gaps)}")

    staleness = []
    for path in sorted(DATA_DIR.glob("readings_*.csv"), key=lambda p: p.stat().st_mtime)[-1:]:
        with path.open() as f:
            staleness = [float(r["staleness_s"]) for r in csv.DictReader(f) if r["staleness_s"]]
    if staleness:
        q = statistics.quantiles(staleness, n=20)
        print(f"Staleness in {path.name} (s): median {statistics.median(staleness):.0f}, p95 {q[18]:.0f}, rows {len(staleness)}")


COMMANDS = {"sample": cmd_sample, "once": cmd_once, "run": cmd_run, "status": cmd_status}

if __name__ == "__main__":
    load_env()
    if len(sys.argv) != 2 or sys.argv[1] not in COMMANDS:
        sys.exit(__doc__)
    COMMANDS[sys.argv[1]]()
