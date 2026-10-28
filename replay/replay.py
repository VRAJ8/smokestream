#!/usr/bin/env python3
"""Replay historical PurpleAir data as if it were arriving live.

Each reading gets an arrival time = event time + a delay drawn from the delay
profile, and sensors drop out for stretches drawn from the profile's outage
statistics. Readings are emitted in arrival order, optionally sped up, to Kafka
or to a folder of JSON files. The folder mode is the fallback if Kafka is not
available: the Spark job can read it with the same code.

The input must be sorted by time_stamp. Memory use stays small because readings
wait in a heap only until no later reading can arrive before them.

Usage:
  python replay/replay.py --history H.csv --sensors S.csv --sink files --out data/replay/run1 --file-seconds 60
  python replay/replay.py --history H.csv --sensors S.csv --sink kafka --topic purpleair --speed 100

--start/--end replay only a slice of arrival time. Delays are drawn for the
whole history either way, so a slice matches the same hours of a full run.
"""

import argparse
import csv
from datetime import datetime, timezone
import heapq
import json
import os
import random
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from common.history import MESSAGE_FIELDS, SENSOR_COLUMNS, resolve_columns  # noqa: E402


class DelayModel:
    """Samples arrival delays and sensor outages from a delay profile."""

    def __init__(self, profile, rng):
        self.rng = rng
        self.quantiles = profile["staleness_quantiles_s"] if profile else None
        self.outage_rate = profile["outage_rate_per_s"] if profile else 0.0
        self.durations = profile["outage_durations_s"] if profile else []
        self.out_until = {}    # sensor -> end of its current outage
        self.last_event = {}   # sensor -> previous reading's event time

    def delay(self):
        if not self.quantiles:
            return 0.0
        # Inverse-CDF sampling, interpolating between neighbouring percentiles.
        u = self.rng.random() * (len(self.quantiles) - 1)
        i = int(u)
        lo, hi = self.quantiles[i], self.quantiles[min(i + 1, len(self.quantiles) - 1)]
        return lo + (hi - lo) * (u - i)

    def dropped(self, sensor, event_time):
        if event_time < self.out_until.get(sensor, float("-inf")):
            return True
        dt = event_time - self.last_event.get(sensor, event_time)
        self.last_event[sensor] = event_time
        if self.durations and dt > 0 and self.rng.random() < self.outage_rate * dt:
            self.out_until[sensor] = event_time + self.rng.choice(self.durations)
            return True
        return False


def load_sensors(path):
    with open(path) as f:
        return {int(r[SENSOR_COLUMNS["sensor_index"]]): (float(r[SENSOR_COLUMNS["lat"]]), float(r[SENSOR_COLUMNS["lon"]]))
                for r in csv.DictReader(f)}


def as_float(value):
    return float(value) if value not in ("", None) else None


def read_history(path, sensors):
    """Yield readings as message dicts (without arrival_time) in event-time order."""
    with open(path) as f:
        reader = csv.DictReader(f)
        cols = resolve_columns(reader.fieldnames)
        previous = float("-inf")
        for row in reader:
            sensor = int(row[cols["sensor_index"]])
            if sensor not in sensors:
                continue
            event_time = float(row[cols["event_time"]])
            if event_time < previous:
                raise SystemExit(f"{path} is not sorted by time_stamp (row with {event_time} after {previous}).")
            previous = event_time
            lat, lon = sensors[sensor]
            yield {"sensor_index": sensor, "event_time": event_time, "lat": lat, "lon": lon,
                   "pm25_a": as_float(row[cols["pm25_a"]]), "pm25_b": as_float(row[cols["pm25_b"]]),
                   "pm1": as_float(row[cols["pm1"]]), "rh": as_float(row[cols["rh"]])}


def arrivals(readings, model, interval_s=0.0):
    """Yield readings in arrival order. Arrival is never before event time.

    interval_s: for averaged history (e.g. 10-minute averages stamped at the start
    of their interval), a reading cannot exist until its interval has ended, so it
    arrives at event_time + interval_s + delay.
    """
    heap, seq = [], 0
    for r in readings:
        if model.dropped(r["sensor_index"], r["event_time"]):
            continue
        # Nothing read later can arrive before this reading's event time.
        while heap and heap[0][0] <= r["event_time"] + interval_s:
            yield heapq.heappop(heap)[2]
        r["arrival_time"] = round(r["event_time"] + interval_s + model.delay(), 3)
        heapq.heappush(heap, (r["arrival_time"], seq, r))
        seq += 1
    while heap:
        yield heapq.heappop(heap)[2]


class FileSink:
    """Writes one JSON-lines file per slice of simulated arrival time.

    Spark judges lateness once per micro-batch, so each batch must cover a short,
    fixed span of simulated time or late readings slip through unnoticed. With
    maxFilesPerTrigger=1 each file is one micro-batch covering file_seconds of
    arrivals, which also makes runs deterministic regardless of machine speed.
    Files appear atomically (write, then rename) so Spark never reads a partial one.
    """

    def __init__(self, out_dir, file_seconds):
        self.dir = Path(out_dir)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.file_seconds = file_seconds
        self.batch, self.slice, self.n_files = [], None, 0
        self.base_mtime = time.time()

    def send(self, msg):
        current = int(msg["arrival_time"] // self.file_seconds)
        if self.slice is not None and current != self.slice:
            self.flush()
        self.slice = current
        self.batch.append(msg)

    def flush(self):
        if not self.batch:
            return
        # Zero-padded slice start time keeps files in arrival order when sorted by name.
        name = f"part-{self.slice * self.file_seconds:012d}.json"
        tmp = self.dir / f".{name}.tmp"
        tmp.write_text("".join(json.dumps(m, separators=(",", ":")) + "\n" for m in self.batch))
        tmp.rename(self.dir / name)
        # Spark's file source reads files oldest-first by modification time, which it
        # sees with millisecond precision. Files written within the same millisecond
        # could be read out of order, so give each one its own increasing timestamp.
        stamp = self.base_mtime + self.n_files * 0.002
        os.utime(self.dir / name, (stamp, stamp))
        self.batch, self.n_files = [], self.n_files + 1

    def close(self):
        self.flush()


class KafkaSink:
    def __init__(self, bootstrap, topic):
        from kafka import KafkaProducer
        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers=bootstrap, linger_ms=50,
                                      value_serializer=lambda m: json.dumps(m, separators=(",", ":")).encode())

    def send(self, msg):
        self.producer.send(self.topic, key=str(msg["sensor_index"]).encode(), value=msg)

    def close(self):
        self.producer.flush()
        self.producer.close()


def utc_seconds(text):
    return datetime.fromisoformat(text).replace(tzinfo=timezone.utc).timestamp()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--history", required=True, help="PurpleAir history CSV, sorted by time_stamp")
    parser.add_argument("--sensors", required=True, help="CSV with sensor_index, latitude, longitude")
    parser.add_argument("--profile", default=str(ROOT / "data" / "delay_profile.json"),
                        help="delay profile JSON; use 'none' for no delays or dropouts")
    parser.add_argument("--sink", choices=["files", "kafka"], default="files")
    parser.add_argument("--out", default=str(ROOT / "data" / "replay" / "run"), help="output folder for --sink files")
    parser.add_argument("--file-seconds", type=int, default=60,
                        help="simulated seconds of arrivals per file (= per Spark micro-batch) for --sink files")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="purpleair")
    parser.add_argument("--speed", type=float, default=0,
                        help="replay speed vs real time (100 = 100x); 0 = as fast as possible")
    parser.add_argument("--seed", type=int, default=226)
    parser.add_argument("--interval-seconds", type=float, default=0,
                        help="averaging interval of the history (600 for 10-minute averages); added to arrivals")
    parser.add_argument("--meta", help="write timing metadata (start time, speed, counts) here, for latency measurement")
    parser.add_argument("--start", type=utc_seconds, help="first arrival time to send, UTC (e.g. 2020-09-11T13:00)")
    parser.add_argument("--end", type=utc_seconds, help="stop before this arrival time, UTC")
    args = parser.parse_args()

    profile = None if args.profile == "none" else json.loads(Path(args.profile).read_text())
    model = DelayModel(profile, random.Random(args.seed))
    sink = FileSink(args.out, args.file_seconds) if args.sink == "files" else KafkaSink(args.bootstrap, args.topic)

    sent, start_wall, first_arrival = 0, time.time(), None
    try:
        for msg in arrivals(read_history(args.history, load_sensors(args.sensors)), model, args.interval_seconds):
            if args.start is not None and msg["arrival_time"] < args.start:
                continue
            if args.end is not None and msg["arrival_time"] >= args.end:
                break
            if args.speed > 0:
                first_arrival = first_arrival if first_arrival is not None else msg["arrival_time"]
                wait = start_wall + (msg["arrival_time"] - first_arrival) / args.speed - time.time()
                if wait > 0:
                    time.sleep(wait)
            sink.send({k: msg[k] for k in MESSAGE_FIELDS})
            sent += 1
            if sent % 100000 == 0:
                print(f"{sent:,} messages", flush=True)
    finally:
        sink.close()
    print(f"Done: {sent:,} messages in {time.time() - start_wall:.1f} s")
    if args.meta:
        # Maps simulated arrival time to wall-clock send time: start_wall + (arrival - first_arrival) / speed.
        Path(args.meta).write_text(json.dumps({"start_wall": start_wall, "first_arrival": first_arrival,
                                               "speed": args.speed, "messages": sent, "finished_wall": time.time(),
                                               "sink": args.sink}, indent=2))


if __name__ == "__main__":
    main()
