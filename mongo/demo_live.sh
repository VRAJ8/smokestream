#!/usr/bin/env bash
# Live-store demo: replays the smoky night and morning of Sept 10-11, 2020 through
# Kafka -> Spark Structured Streaming -> MongoDB Atlas, querying the store as it fills.
#
# Usage: bash mongo/demo_live.sh            (14 simulated hours at 120x, about 8 minutes)
#        SPEED=60 bash mongo/demo_live.sh   (slower, easier to watch live)
# Needs Kafka on localhost:9092 and MONGODB_URI in .env. Hexagons stay in the store
# for 24 h after the run (TTL index), so run it shortly before showing it.
set -euo pipefail
cd "$(dirname "$0")/.."
PY=.venv/bin/python
KAFKA=/opt/homebrew/opt/kafka/bin
TOPIC=purpleair_demo
OUT=data/out/demo_live
SPEED=${SPEED:-120}
START=${START:-2020-09-11T05:00}
END=${END:-2020-09-11T19:00}
PROFILE=data/delay_profile.json
[ -f $PROFILE ] || PROFILE=data/delay_profile_partial.json

nc -z localhost 9092 2>/dev/null || { echo "Kafka is not running on localhost:9092. Start it with: brew services start kafka"; exit 1; }

# Fresh topic, output folder and live store, so nothing from an earlier run leaks in.
$KAFKA/kafka-topics --bootstrap-server localhost:9092 --delete --topic $TOPIC 2>/dev/null || true
for _ in $(seq 30); do  # deletion finishes in the background
  $KAFKA/kafka-topics --bootstrap-server localhost:9092 --create --topic $TOPIC \
    --partitions 4 --replication-factor 1 >/dev/null 2>&1 && break
  sleep 1
done
rm -rf $OUT && mkdir -p $OUT
$PY mongo/setup_indexes.py --reset

$PY spark/streaming_job.py --source kafka --topic $TOPIC --context data/history/2020/context \
  --out $OUT/stream > $OUT/stream.log 2>&1 &
JOB=$!
trap 'kill $JOB 2>/dev/null || true' EXIT
until grep -q "running" $OUT/stream.log; do
  kill -0 $JOB 2>/dev/null || { echo "Streaming job failed; see $OUT/stream.log"; exit 1; }
  sleep 1
done
echo "Streaming job is up. Replaying $START to $END UTC at ${SPEED}x."

$PY replay/replay.py --history data/history/2020/history_avg10.csv --sensors data/history/2020/sensors.csv \
  --profile $PROFILE --interval-seconds 600 --sink kafka --topic $TOPIC \
  --speed "$SPEED" --start "$START" --end "$END" --meta $OUT/replay_meta.json > $OUT/replay.log 2>&1 &
REPLAY=$!
while kill -0 $REPLAY 2>/dev/null; do
  sleep 90
  echo; $PY mongo/query_live.py near
done
wait $REPLAY

sleep 30  # let the job finish the last few batches
echo; $PY mongo/query_live.py summary
echo; $PY mongo/query_live.py near
echo; $PY analysis/measure_streaming.py --output $OUT/stream --config w10_wm5_q120_c60_r7 --meta $OUT/replay_meta.json \
  | tee $OUT/measure.json
echo; $PY analysis/latency_split.py --output $OUT/stream --config w10_wm5_q120_c60_r7 --meta $OUT/replay_meta.json \
  | tee $OUT/latency_split.json
echo; echo "Done. Parquet output in $OUT/stream, logs in $OUT."
