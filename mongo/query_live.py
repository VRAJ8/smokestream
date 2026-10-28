#!/usr/bin/env python3
"""Ask the live store what the air is like right now.

  near     hexagons within --km of a point, nearest first ($geoNear on the 2dsphere index)
  summary  hexagon counts by AQI category and smoke label, and the worst hexagons

Times are shown in Pacific time. "data time" is the end of the hexagon's latest
window; during a replay it is the replayed (historical) time, not today.

Usage:
  python mongo/query_live.py near                      # around SJSU
  python mongo/query_live.py near --lat 37.77 --lon -122.42 --km 10
  python mongo/query_live.py summary
"""

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import pandas as pd  # noqa: E402
from pymongo import MongoClient  # noqa: E402

from common.purpleair import load_env  # noqa: E402

SJSU = (37.3352, -121.8811)
COLUMNS = {"h3": "hexagon", "pm25": "PM2.5", "aqi": "AQI", "aqi_category": "category",
           "smoke_label": "smoke", "smoke_alert": "alert", "n_sensors": "sensors", "window_end": "data time"}


def table(docs, first=()):
    if not docs:
        return "(no hexagons)"
    df = pd.DataFrame(docs)
    df["window_end"] = pd.to_datetime(df["window_end"], utc=True).dt.tz_convert("America/Los_Angeles").dt.strftime("%b %d %H:%M")
    df["pm25"] = df["pm25"].round(1)
    df["aqi"] = df["aqi"].round().astype("Int64")
    return df[[*first, *COLUMNS]].rename(columns=COLUMNS).to_string(index=False)


def near(hexes, lat, lon, km):
    docs = list(hexes.aggregate([
        {"$geoNear": {"near": {"type": "Point", "coordinates": [lon, lat]},
                      "distanceField": "dist_m", "maxDistance": km * 1000, "spherical": True}},
        {"$project": {"_id": 0, "dist_m": 1, **{c: 1 for c in COLUMNS}}},
    ]))
    for d in docs:
        d["km"] = round(d.pop("dist_m") / 1000, 1)
    print(f"Hexagons within {km:g} km of ({lat}, {lon}): {len(docs)}")
    print(table(docs, first=["km"]))


def summary(hexes):
    total = hexes.count_documents({})
    print(f"Hexagons in the live store: {total}")
    if not total:
        return
    by = list(hexes.aggregate([
        {"$group": {"_id": {"category": "$aqi_category", "smoke": "$smoke_label"}, "hexagons": {"$sum": 1},
                    "alerts": {"$sum": {"$cond": ["$smoke_alert", 1, 0]}}}},
    ]))
    counts = pd.DataFrame([{**b["_id"], "hexagons": b["hexagons"], "alerts": b["alerts"]} for b in by])
    print(counts.pivot_table(index="category", columns="smoke", values="hexagons", aggfunc="sum", fill_value=0)
          .to_string())
    print(f"Smoke alerts active: {int(counts['alerts'].sum())}")
    print("\nWorst hexagons:")
    print(table(list(hexes.find({}, {"_id": 0, **{c: 1 for c in COLUMNS}}).sort("aqi", -1).limit(5))))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("what", choices=["near", "summary"])
    parser.add_argument("--lat", type=float, default=SJSU[0])
    parser.add_argument("--lon", type=float, default=SJSU[1])
    parser.add_argument("--km", type=float, default=10.0)
    args = parser.parse_args()

    load_env(ROOT)
    if not os.environ.get("MONGODB_URI"):
        sys.exit("Set MONGODB_URI in .env first (see mongo/setup_indexes.py).")
    with MongoClient(os.environ["MONGODB_URI"]) as client:
        hexes = client[os.environ.get("MONGODB_DB", "smokestream")]["hex_state"]
        near(hexes, args.lat, args.lon, args.km) if args.what == "near" else summary(hexes)


if __name__ == "__main__":
    main()
