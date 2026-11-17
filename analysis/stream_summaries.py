#!/usr/bin/env python3
"""Streaming algorithms on the replayed sensor stream, checked against exact answers.

Reads messages in arrival order, from a replay folder or from Kafka, and runs the
summaries in common/stream_algos.py side by side with exact (unbounded-memory)
versions:

  dedupe     rotating Bloom filter on (sensor, event time). A share of messages is
             delivered twice, a few minutes apart, as Kafka's at-least-once delivery
             does after a producer retry or a consumer restart.
  distinct   Flajolet-Martin count of sensors reporting in each event-time hour
  elevated   DGIM count of readings above 35.4 ug/m3 (AQI above 100) among the last N
  sample     reservoir sample of Unhealthy readings, repeated over many seeds to check
             that every position in the stream is equally likely to be kept

Usage:
  python analysis/stream_summaries.py --replay data/out/season_2020/replay_measured \\
      --out data/out/season_2020/stream_summaries.json
  python analysis/stream_summaries.py --kafka purpleair_demo      # live, prints as it goes
"""

import argparse
import heapq
import json
import random
import sys
from collections import deque
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from common import airquality as aq  # noqa: E402
from common.stream_algos import DGIM, FlajoletMartin, ReservoirSample, RotatingBloom  # noqa: E402

ELEVATED = 35.4    # ug/m3, top of "Moderate" (AQI 100)
UNHEALTHY = 55.4   # AQI 150


def from_replay(folder):
    for path in sorted(Path(folder).glob("*.json")):
        with path.open() as f:
            batch = [json.loads(line) for line in f if line.strip()]
        yield from sorted(batch, key=lambda m: m["arrival_time"])


def from_kafka(topic, servers, idle_s):
    from kafka import KafkaConsumer
    consumer = KafkaConsumer(topic, bootstrap_servers=servers, auto_offset_reset="earliest",
                             consumer_timeout_ms=idle_s * 1000, value_deserializer=json.loads)
    for record in consumer:
        yield record.value


def with_redeliveries(messages, rate, max_delay_s, seed):
    """Deliver a share of messages a second time, up to max_delay_s later (arrival order kept)."""
    rng, pending = random.Random(seed), []
    for m in messages:
        while pending and pending[0][0] <= m["arrival_time"]:
            yield heapq.heappop(pending)[2]
        yield m
        if rng.random() < rate:
            copy = dict(m, arrival_time=m["arrival_time"] + rng.uniform(1, max_delay_s), duplicate=True)
            heapq.heappush(pending, (copy["arrival_time"], id(copy), copy))
    while pending:
        yield heapq.heappop(pending)[2]


def corrected(m):
    a, b, rh = m.get("pm25_a"), m.get("pm25_b"), m.get("rh")
    if not (aq.is_valid(a, b, rh) and aq.ab_agree(a, b)):
        return None
    return aq.epa_correct((a + b) / 2, rh)


def run(messages, dgim_n=(1000, 10000), bloom_capacity=400, bloom_period_s=3600, live_every=0):
    bloom, seen = RotatingBloom(bloom_capacity, bloom_period_s), set()
    dup = {"delivered": 0, "duplicates": 0, "caught": 0, "missed": 0, "false_drops": 0}
    hours = {}                     # hour -> (FlajoletMartin, exact set)
    dgims = {n: (DGIM(n), deque(maxlen=n), []) for n in dgim_n}
    unhealthy = []                 # the stream the reservoir samples from
    for i, m in enumerate(messages):
        key = (m["sensor_index"], m["event_time"])
        dup["delivered"] += 1
        truly_dup = key in seen
        dup["duplicates"] += truly_dup
        dropped = bloom.seen_before(key, m["arrival_time"])
        if truly_dup:
            dup["caught" if dropped else "missed"] += 1
        elif dropped:
            dup["false_drops"] += 1
        seen.add(key)
        if dropped:
            continue

        hour = int(m["event_time"] // 3600 * 3600)
        if hour not in hours:
            hours[hour] = (FlajoletMartin(), set())
        hours[hour][0].add(m["sensor_index"])
        hours[hour][1].add(m["sensor_index"])

        pm = corrected(m)
        if pm is None:
            continue
        for n, (dg, window, checks) in dgims.items():
            dg.add(pm > ELEVATED)
            window.append(pm > ELEVATED)
            if len(window) == n and dup["delivered"] % 250 == 0:
                checks.append((dg.count(), sum(window), dg.nbytes))
        if pm > UNHEALTHY:
            unhealthy.append(key)
        if live_every and i % live_every == 0:
            fm, exact = hours[hour]
            print(f"{i:>8,} msgs  dupes dropped {dup['caught']:,}  sensors this hour ~{fm.estimate():.0f} "
                  f"(exact {len(exact)})  elevated in last {dgim_n[0]}: ~{dgims[dgim_n[0]][0].count():.0f}", flush=True)
    return dup, hours, dgims, unhealthy, bloom


def summarize(dup, hours, dgims, unhealthy, bloom, k=50, seeds=1000):
    fm_err = [abs(fm.estimate() - len(ex)) / len(ex) for fm, ex in hours.values() if ex]
    out = {
        "dedupe": {**dup, "bloom_bytes": bloom.nbytes,
                   "exact_bytes_per_hour": 16 * round(dup["delivered"] / max(1, len(hours))),  # two 8-byte numbers per key
                   "bloom_bits_per_key": round(bloom.current.m / bloom.capacity, 1)},
        "distinct_sensors": {
            "hours": len(fm_err),
            "mean_rel_error": round(sum(fm_err) / len(fm_err), 3),
            "p90_rel_error": round(sorted(fm_err)[int(0.9 * len(fm_err))], 3),
            "fm_bytes": FlajoletMartin().nbytes,
        },
        "elevated_last_n": {},
        "reservoir": {},
    }
    for n, (_, _, checks) in dgims.items():
        errs = [abs(est - ex) / ex for est, ex, _ in checks if ex > 0]
        out["elevated_last_n"][str(n)] = {
            "checks": len(checks), "mean_rel_error": round(sum(errs) / len(errs), 3) if errs else None,
            "max_rel_error": round(max(errs), 3) if errs else None,
            "dgim_bytes_max": max(b for _, _, b in checks) if checks else None, "exact_bytes": n // 8}
    n = len(unhealthy)
    if n > k:
        counts = [0] * 10
        for seed in range(seeds):
            r = ReservoirSample(k, seed)
            for pos in range(n):
                r.add(pos)
            for pos in r.items:
                counts[pos * 10 // n] += 1
        share = [c / (k * seeds) for c in counts]
        out["reservoir"] = {"stream_items": n, "k": k, "seeds": seeds,
                            "share_by_stream_decile": [round(s, 4) for s in share],
                            "max_deviation_from_0.1": round(max(abs(s - 0.1) for s in share), 4)}
    return out


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument("--replay", help="replay folder (one JSON-lines file per simulated minute)")
    src.add_argument("--kafka", help="Kafka topic to consume")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--idle-seconds", type=int, default=30, help="Kafka: stop after this long without messages")
    parser.add_argument("--dup-rate", type=float, default=0.02)
    parser.add_argument("--max-redelivery-s", type=float, default=600)
    parser.add_argument("--seed", type=int, default=226)
    parser.add_argument("--out")
    args = parser.parse_args()

    if args.replay:
        source = from_replay(args.replay)
    else:
        source = from_kafka(args.kafka, args.bootstrap, args.idle_seconds)
    messages = with_redeliveries(source, args.dup_rate, args.max_redelivery_s, args.seed)
    result = summarize(*run(messages, live_every=0 if args.replay else 500))
    text = json.dumps(result, indent=2)
    print(text)
    if args.out:
        Path(args.out).write_text(text)


if __name__ == "__main__":
    main()
