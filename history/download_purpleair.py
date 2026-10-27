#!/usr/bin/env python3
"""Download historical PurpleAir data for a fire season, within a points budget.

History costs points for every row and field, so this tool always estimates
first, never spends past --max-points, and can resume where it stopped.
Output matches what replay/replay.py and spark/batch_job.py expect.

Steps (run in order; each reads what the previous one saved):
  sensors   pick sensors that were active during the season (one cheap call)
  estimate  rows and points the download will cost; spends nothing
  download  fetch history in chunks, one sensor at a time, until done or out of budget
  merge     combine everything into one history.csv sorted by time, plus sensors.csv
  status    what has been downloaded and spent so far

Examples:
  python history/download_purpleair.py sensors  --season 2020 --sample 60
  python history/download_purpleair.py estimate --season 2020 --average 10
  python history/download_purpleair.py download --season 2020 --average 10 --max-points 900000
  python history/download_purpleair.py merge    --season 2020

Points: requests cost a 2-point base plus (cost of the requested fields x rows).
Field prices are not published in one place, so --points-per-row is an estimate.
Check it once: run download with --max-requests 1, see what that call cost on
develop.purpleair.com, divide by the rows it printed, and pass that number.
"""

import argparse
import csv
import json
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from common import purpleair as pa  # noqa: E402

HISTORY_DIR = ROOT / "data" / "history"

# Season windows in UTC; end is exclusive.
SEASONS = {
    "2020": ("2020-08-01", "2020-10-01"),  # SCU, LNU and CZU Lightning Complex fires
    "2021": ("2021-07-01", "2021-09-16"),  # Dixie and Caldor fires, includes July 4
    "2024": ("2024-07-01", "2024-09-16"),  # Park Fire, includes July 4
}

# west, south, east, north
REGIONS = {
    "bayarea": (-123.0, 36.9, -121.2, 38.3),
    "norcal": (-124.0, 36.5, -119.5, 41.0),
}

# Channel-level fields (cheaper than averaged ones). Names match common/history.py.
FIELDS = ["pm2.5_cf_1_a", "pm2.5_cf_1_b", "pm1.0_cf_1_a", "humidity_a"]

# Longest span per request for each average (minutes). These are the most
# conservative limits PurpleAir has published; later limits are longer.
MAX_SPAN_DAYS = {0: 2, 10: 3, 30: 7, 60: 14}

BASE_POINTS = 2
REQUEST_GAP_S = 1.1  # PurpleAir asks for at least 1 s between history calls


def ts(date_str):
    return int(datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())


def season_dir(season):
    return HISTORY_DIR / season


def load_meta(season):
    path = season_dir(season) / "season.json"
    if not path.exists():
        sys.exit(f"Run 'sensors --season {season}' first.")
    return json.loads(path.read_text())


def load_sensor_ids(season):
    with (season_dir(season) / "sensors.csv").open() as f:
        return [int(r["sensor_index"]) for r in csv.DictReader(f)]


def chunks(start, end, average):
    span = MAX_SPAN_DAYS[average] * 86400
    t = start
    while t < end:
        yield t, min(t + span, end)  # end is exclusive, so chunks never overlap
        t += span


def to_epoch(value):
    """History timestamps come back in the format requested (we send UNIX seconds),
    but accept ISO 8601 too in case the API answers that way."""
    try:
        return int(float(value))
    except ValueError:
        return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp())


def rows_per_sensor(start, end, average):
    step = 120 if average == 0 else average * 60  # real-time readings arrive every 2 minutes
    return (end - start) // step


# ---------- commands ----------

SENSOR_FIELDS = ["sensor_index", "latitude", "longitude", "date_created", "last_seen"]


def _km(lat1, lon1, lat2, lon2):
    import math
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = (math.sin((p2 - p1) / 2) ** 2
         + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2)
    return 12742 * math.asin(math.sqrt(a))


def _write_sensors(path, sensors):
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=SENSOR_FIELDS, extrasaction="ignore")
        w.writeheader()
        w.writerows(sorted(sensors, key=lambda s: int(s["sensor_index"])))


def _downloaded_ids(out):
    """Sensors that already have history chunks on disk (any resolution)."""
    return {int(p.name) for d in out.glob("raw_avg*") for p in d.iterdir() if p.is_dir() and any(p.iterdir())}


def anchored(candidates, monitors_path, per_monitor, radius_km, already=()):
    """Up to per_monitor sensors around each monitor (closest first, within radius_km),
    so every monitor gets nearby sensors and each cluster supports neighbour checks.
    Sensors in `already` (e.g. downloaded earlier) count toward a monitor's quota,
    so no points are spent on monitors that are already covered."""
    sites = {}
    with open(monitors_path) as f:
        for r in csv.DictReader(f):
            sites[r["site_id"]] = (float(r["latitude"]), float(r["longitude"]))
    chosen, have = {}, set(already)
    for site, (lat, lon) in sorted(sites.items()):
        near = sorted((_km(lat, lon, c["latitude"], c["longitude"]), int(c["sensor_index"])) for c in candidates)
        in_range = [idx for d, idx in near if d <= radius_km]
        need = per_monitor - sum(1 for idx in in_range if idx in have)
        picks = [idx for idx in in_range if idx not in have and idx not in chosen][:max(0, need)]
        for idx in picks:
            chosen[idx] = site
    return chosen


def cmd_sensors(args):
    start_s, end_s = args.start or SEASONS[args.season][0], args.end or SEASONS[args.season][1]
    start, end = ts(start_s), ts(end_s)
    out = season_dir(args.season)
    out.mkdir(parents=True, exist_ok=True)
    cand_path = out / f"sensors_candidates_{args.region}.csv"
    if cand_path.exists() and args.reuse_candidates:
        with cand_path.open() as f:
            everyone = [{k: float(v) if k != "sensor_index" else int(v) for k, v in r.items()} for r in csv.DictReader(f)]
    else:
        west, south, east, north = REGIONS[args.region]
        payload = pa.get("/sensors", {
            "fields": "latitude,longitude,date_created,last_seen",
            "location_type": 0,   # outdoor
            "max_age": 0,         # include sensors that have since gone offline
            "nwlng": west, "nwlat": north, "selng": east, "selat": south,
        })
        everyone = pa.rows_as_dicts(payload)
        _write_sensors(cand_path, everyone)  # saved so re-sampling costs no points
    candidates = [s for s in everyone
                  if s.get("date_created") and s["date_created"] < end
                  and s.get("last_seen") and s["last_seen"] >= start]

    by_id = {int(c["sensor_index"]): c for c in candidates}
    if args.near_monitors:
        keep = _downloaded_ids(out) if args.keep_downloaded else set()
        picks = anchored(candidates, args.near_monitors, args.per_monitor, args.radius_km, already=keep)
        chosen = [by_id[i] for i in sorted(keep | set(picks)) if i in by_id]
        how = (f"{len(picks)} anchored on monitors ({args.per_monitor} per monitor within {args.radius_km} km)"
               + (f" plus {len(keep - set(picks))} already downloaded" if keep else ""))
    else:
        random.Random(args.seed).shuffle(candidates)
        chosen = candidates[:args.sample] if args.sample else candidates
        how = f"{len(chosen)} at random (seed {args.seed})"

    if (out / "sensors.csv").exists():
        (out / "sensors.csv").replace(out / "sensors_previous.csv")
    _write_sensors(out / "sensors.csv", chosen)
    (out / "season.json").write_text(json.dumps({
        "season": args.season, "start": start, "end": end, "start_date": start_s, "end_date": end_s,
        "region": args.region, "sample": args.sample, "seed": args.seed, "candidates": len(candidates),
        "selection": "monitor-anchored" if args.near_monitors else "random",
    }, indent=2))
    print(f"{len(candidates)} outdoor sensors in {args.region} were active during {start_s} to {end_s}; "
          f"kept {len(chosen)}: {how}.")
    print(f"Saved {out / 'sensors.csv'}. Note: locations are today's; sensors that moved since may be off.")


def estimate(meta, n_sensors, average, points_per_row):
    rows = rows_per_sensor(meta["start"], meta["end"], average) * n_sensors
    requests = len(list(chunks(meta["start"], meta["end"], average))) * n_sensors
    return rows, requests, rows * points_per_row + requests * BASE_POINTS


def cmd_estimate(args):
    meta, ids = load_meta(args.season), load_sensor_ids(args.season)
    rows, requests, points = estimate(meta, len(ids), args.average, args.points_per_row)
    print(f"Season {args.season}: {meta['start_date']} to {meta['end_date']}, {len(ids)} sensors, "
          f"average={args.average} min")
    print(f"  up to {rows:,} rows in {requests:,} requests")
    print(f"  about {points:,.0f} points at {args.points_per_row} points per row "
          f"(about ${points / 100_000:,.2f} beyond any free points)")
    print("  Offline sensors return fewer rows, so the real cost is usually lower.")


def cmd_download(args):
    meta, ids = load_meta(args.season), load_sensor_ids(args.season)
    raw = season_dir(args.season) / f"raw_avg{args.average}"
    ledger_path = raw / "ledger.json"
    raw.mkdir(parents=True, exist_ok=True)
    ledger = json.loads(ledger_path.read_text()) if ledger_path.exists() else {"points": 0, "requests": 0, "rows": 0}

    worst_case_chunk = rows_per_sensor(0, MAX_SPAN_DAYS[args.average] * 86400, args.average) * args.points_per_row
    done_now = 0
    for sensor in ids:
        for start, end in chunks(meta["start"], meta["end"], args.average):
            path = raw / str(sensor) / f"{start}.csv"
            if path.exists():
                continue  # already downloaded on an earlier run
            if ledger["points"] + BASE_POINTS + worst_case_chunk > args.max_points:
                print(f"Stopping: the next request could pass the {args.max_points:,} point budget.")
                return finish(ledger, ledger_path)
            if args.max_requests and done_now >= args.max_requests:
                print(f"Stopping after {done_now} request(s) (--max-requests).")
                return finish(ledger, ledger_path)

            try:
                payload = pa.get(f"/sensors/{sensor}/history", {
                    "start_timestamp": start, "end_timestamp": end,
                    "average": args.average, "fields": ",".join(FIELDS),
                })
            except pa.PurpleAirError as err:
                print(f"Stopping: {err}")
                return finish(ledger, ledger_path)

            rows = pa.rows_as_dicts(payload)
            write_chunk(path, rows)
            ledger["requests"] += 1
            ledger["rows"] += len(rows)
            ledger["points"] += BASE_POINTS + len(rows) * args.points_per_row
            done_now += 1
            print(f"sensor {sensor}  {datetime.fromtimestamp(start, timezone.utc):%Y-%m-%d}  "
                  f"{len(rows):>4} rows  ~{ledger['points']:,.0f} points so far", flush=True)
            time.sleep(REQUEST_GAP_S)
    print("All chunks downloaded.")
    finish(ledger, ledger_path)


def write_chunk(path, rows):
    """Write atomically; an empty chunk still gets a file so it is not re-requested."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["time_stamp", *FIELDS], extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    tmp.rename(path)


def finish(ledger, ledger_path):
    ledger_path.write_text(json.dumps(ledger, indent=2))
    print(f"Estimated spend so far: {ledger['points']:,.0f} points over {ledger['requests']:,} requests, "
          f"{ledger['rows']:,} rows.")


def cmd_merge(args):
    meta, ids = load_meta(args.season), load_sensor_ids(args.season)
    out_dir = season_dir(args.season)
    raw = out_dir / f"raw_avg{args.average}"
    if not raw.exists():
        sys.exit(f"Nothing downloaded for average={args.average}.")

    total, missing = 0, 0
    out_path = out_dir / f"history_avg{args.average}.csv"
    with out_path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["time_stamp", "sensor_index", *FIELDS])
        # Chunks line up across sensors, so merging chunk by chunk keeps memory small
        # and the output sorted by time.
        for start, _ in chunks(meta["start"], meta["end"], args.average):
            rows = {}
            for sensor in ids:
                path = raw / str(sensor) / f"{start}.csv"
                if not path.exists():
                    missing += 1
                    continue
                with path.open() as cf:
                    for r in csv.DictReader(cf):
                        rows[(to_epoch(r["time_stamp"]), sensor)] = [r[c] for c in FIELDS]  # dedupes
            for (t, sensor) in sorted(rows):
                w.writerow([t, sensor, *rows[(t, sensor)]])
            total += len(rows)

    with out_path.open() as f:
        n_sensors = len({r["sensor_index"] for r in csv.DictReader(f)})
    print(f"Wrote {out_path} ({total:,} rows from {n_sensors} sensors with data).")
    if missing:
        print(f"  {missing} sensor-chunks are not downloaded yet; they are simply absent from the file.")
    print(f"Use with: --history {out_path} --sensors {out_dir / 'sensors.csv'}")


def cmd_status(args):
    meta, ids = load_meta(args.season), load_sensor_ids(args.season)
    raw = season_dir(args.season) / f"raw_avg{args.average}"
    total = len(ids) * len(list(chunks(meta["start"], meta["end"], args.average)))
    done = len(list(raw.glob("*/*.csv"))) if raw.exists() else 0
    print(f"Season {args.season}, average={args.average}: {done:,} of {total:,} sensor-chunks downloaded "
          f"({100 * done / total:.0f}%).")
    ledger_path = raw / "ledger.json"
    if ledger_path.exists():
        ledger = json.loads(ledger_path.read_text())
        print(f"Estimated spend: {ledger['points']:,.0f} points, {ledger['rows']:,} rows.")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    def add(name, func):
        p = sub.add_parser(name)
        p.add_argument("--season", required=True, choices=sorted(SEASONS))
        p.set_defaults(func=func)
        return p

    p = add("sensors", cmd_sensors)
    p.add_argument("--region", choices=sorted(REGIONS), default="bayarea")
    p.add_argument("--sample", type=int, default=60, help="sensors to keep; 0 keeps all")
    p.add_argument("--seed", type=int, default=226)
    p.add_argument("--start", help="override season start, YYYY-MM-DD (e.g. a peak smoke week)")
    p.add_argument("--end", help="override season end, YYYY-MM-DD (exclusive)")
    p.add_argument("--near-monitors", help="monitor CSV: pick sensors around each monitor instead of at random")
    p.add_argument("--per-monitor", type=int, default=1)
    p.add_argument("--radius-km", type=float, default=3.0)
    p.add_argument("--keep-downloaded", action="store_true", help="also keep sensors already downloaded")
    p.add_argument("--reuse-candidates", action="store_true",
                   help="reuse the saved candidate list instead of calling the API (no points)")

    for name, func in [("estimate", cmd_estimate), ("download", cmd_download),
                       ("merge", cmd_merge), ("status", cmd_status)]:
        p = add(name, func)
        p.add_argument("--average", type=int, choices=sorted(MAX_SPAN_DAYS), default=10,
                       help="minutes; 0 = raw 2-minute data (5x the rows of 10)")
        if name in ("estimate", "download"):
            p.add_argument("--points-per-row", type=float, default=6.0,
                           help="estimated points per returned row (see the note above)")
        if name == "download":
            p.add_argument("--max-points", type=int, required=True, help="hard budget for this season")
            p.add_argument("--max-requests", type=int, help="stop after this many requests (for a cost check)")

    args = parser.parse_args()
    pa.load_env(ROOT)
    try:
        args.func(args)
    except pa.PurpleAirError as err:
        sys.exit(str(err))


if __name__ == "__main__":
    main()
