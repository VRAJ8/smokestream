#!/usr/bin/env python3
"""Create the MongoDB indexes the live store needs. Safe to run more than once.

hex_state holds one document per hexagon: its latest finalized window, written
by spark/streaming_job.py when MONGODB_URI is set.

  h3 (unique)        one document per hexagon
  location 2dsphere  "current AQI within 5 km of a point" queries
  emitted_at TTL     hexagons with no new data for 24 h drop off the live map

Usage: python mongo/setup_indexes.py            (reads MONGODB_URI from .env)
       python mongo/setup_indexes.py --reset    also empty hex_state first (demo runs)
Example query (mongosh), hexagons within 5 km of SJSU:
  db.hex_state.find({location: {$near: {$geometry: {type: "Point", coordinates: [-121.8811, 37.3352]},
                                        $maxDistance: 5000}}})
"""

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pymongo import ASCENDING, GEOSPHERE, MongoClient  # noqa: E402

from common.purpleair import load_env  # noqa: E402

TTL_SECONDS = 24 * 3600


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--reset", action="store_true", help="delete all hex_state documents first")
    args = parser.parse_args()
    load_env(ROOT)
    uri = os.environ.get("MONGODB_URI")
    if not uri:
        sys.exit("Set MONGODB_URI (your Atlas connection string) in .env first.")
    with MongoClient(uri, serverSelectionTimeoutMS=10000) as client:
        client.admin.command("ping")
        print("Connected to MongoDB.")
        hexes = client[os.environ.get("MONGODB_DB", "smokestream")]["hex_state"]
        if args.reset:
            print(f"Removed {hexes.delete_many({}).deleted_count} hexagon documents.")
        hexes.create_index([("h3", ASCENDING)], unique=True, name="h3_unique")
        hexes.create_index([("location", GEOSPHERE)], name="location_2dsphere")
        hexes.create_index([("emitted_at", ASCENDING)], expireAfterSeconds=TTL_SECONDS, name="emitted_at_ttl")
        print("Indexes:", ", ".join(hexes.index_information()))


if __name__ == "__main__":
    main()
