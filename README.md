# SmokeStream

Measuring the latency-accuracy trade-off in real-time wildfire smoke detection.
DATA 226 (Big Data and Warehousing), San José State University, Fall 2026.
Team: Vraj Patel, Brandon Moriguchi.

## Layout

```
recorder/    live PurpleAir recorder (one week of data for the arrival-delay profile)
history/     PurpleAir downloader (budgeted, resumable) and the public-data downloader
             (EPA AQS monitors, NASA FIRMS fires, NOAA HMS plumes, ASOS wind; no keys needed)
replay/      delay profile builder and the replay tool (history -> Kafka or files)
common/      shared logic: EPA correction, AQI, Spark transforms, steps 4-5 (enrich),
             context loading, watermark simulator
spark/       streaming job, hindsight batch job, fast simulated streaming job
analysis/    stream-vs-baseline comparison, knob sweep, monitor evaluation, labeling tool, figures
airflow/     DAGs for the slower sources, Snowflake loads, sensor scores (stubs)
sql/         Snowflake star schema
mongo/       MongoDB index setup for the live store
tools/       synthetic data generator for testing
tests/       pytest suite
data/        local data, not committed
```

## How the pieces fit

```
PurpleAir history ──> replay.py ──(delays + dropouts from the live recorder)──> Kafka or JSON files
                                                                                 │
                         ┌───────────────────────────────────────────────────────┤
                         ▼                                                       ▼
              spark/streaming_job.py                                spark/simulate_stream.py
              (real engine; RQ4 speed tests,                        (same output in seconds;
               MongoDB live store)                                   RQ1/RQ2 knob sweeps)
                         │                                                       │
     context (fires, plume cells, wind; Airflow) ──> steps 4-5 in common/enrich.py
                         │                                                       │
                         └──────────────> data/out/stream <──────────────────────┘
                                                │
PurpleAir history ──> spark/batch_job.py ──> data/out/batch (hindsight baseline)
                                                │
   analysis/compare_stream_batch.py, analysis/run_sweep.py ──> results CSV ──> frontier plots
```

All three Spark jobs run the same code. Steps 1-3 and the per-sensor windowed average are in `common/transforms.py`. The neighbour check, hexagon average, AQI, context and smoke labels (steps 4-6) are in `common/enrich.py`. So any difference between streaming and batch comes from real-time constraints (late data, dropouts, context that was not published yet), not from different code.

**Output:** one row per hexagon per 10-minute window: PM2.5 and AQI, how many sensors were used or excluded, `extent` (share of nearby hexagons also elevated), `fine_ratio` (PM1/PM2.5), `in_plume`, `upwind_fire`, wind, `smoke_label` (wildfire / local / uncertain, or empty when air is not elevated) and `smoke_alert` (AQI over 150 and not local).

## Setup (once)

Needs Python 3.12 (PySpark does not support 3.14 yet) and Java 17.

```bash
/opt/homebrew/bin/python3.12 -m venv .venv
```

```bash
.venv/bin/pip install -r requirements.txt
```

Run the tests (about 20 seconds; the slow simulator-vs-Spark check adds about 4 minutes):

```bash
.venv/bin/python -m pytest -m "not slow"
```

```bash
.venv/bin/python -m pytest
```

## Try the whole pipeline on synthetic data

```bash
.venv/bin/python tools/make_synthetic_history.py --out data/synthetic
```

```bash
.venv/bin/python replay/replay.py --history data/synthetic/history.csv --sensors data/synthetic/sensors.csv --profile none --out data/replay/demo --file-seconds 60
```

```bash
.venv/bin/python spark/batch_job.py --history data/synthetic/history.csv --sensors data/synthetic/sensors.csv --context data/synthetic/context --window 10 --qc-history 120
```

```bash
.venv/bin/python spark/simulate_stream.py --input data/replay/demo --context data/synthetic/context --window 10 --watermark 5 --qc-history 120 --context-refresh 60
```

```bash
.venv/bin/python analysis/compare_stream_batch.py --stream-config w10_wm5_q120_c60_r7 --batch-config w10_q120_r7 --results data/out/results.csv
```

With `--profile none` there are no delays, so the stream should match the baseline almost exactly. Once the live recorder has run for a week, build the real profile with `replay/build_delay_profile.py` and drop `--profile none`.

To run the whole knob sweep for a season in one go (one Spark session, about 5 seconds per setting on synthetic data, resumable):

```bash
.venv/bin/python analysis/run_sweep.py --label synth --replay data/replay/demo --history data/synthetic/history.csv --sensors data/synthetic/sensors.csv --context data/synthetic/context --levels window=10,30 --results data/out/sweep.csv
```

By default it varies each knob one at a time around window 10, watermark 5, QC history 120 and context refresh 60 minutes, using the levels from the proposal's Table 2. Add `--grid watermark,qc_history` for a full grid over two knobs.

## Ground truth: regulatory monitors

The pipelines never see monitor data, so this is a held-out test. For every monitor and hour, `common/evaluate.py` estimates PM2.5 only from the sensor hexagons within 5 km (inverse-distance weighted) and compares the estimate with the monitor. It reports:
- **Accuracy:** RMSE, MAE, bias and R² against the monitors.
- **Hourly alerts:** precision, recall and false-alarm rate for "Unhealthy" hours (PM2.5 above 55.4, AQI above 150). There are two versions: the value alone, and smoke alerts that ignore local spikes. The difference shows whether context reduces false alarms.
- **Lead time:** for each Unhealthy episode, how many minutes before the official hourly value was published (end of the hour plus 30 minutes, adjustable) the stream first alerted nearby.

```bash
.venv/bin/python analysis/evaluate_monitors.py --output data/out/batch --monitors data/synthetic/monitors --details data/out/monitor_details
```

To measure what each processing step contributes, compare batch variants: `--no-correction` (raw PurpleAir), `--qc-history 0` (neighbour check off) and the defaults. On synthetic data, the correction cuts the error against monitors from about 26 to 3 µg/m³. Add `--monitors` to `run_sweep.py` to score every setting against monitors, then plot with `--metric mon_rmse_ugm3 --rows monitors`.

Monitor files have the columns `site_id, latitude, longitude, hour_start` (UNIX seconds, UTC), `pm25`. They come from Brandon's AirNow/AQS tasks; the synthetic generator writes a matching `monitors/monitors.csv`.

## RQ3: hand-labeled events

Export about 300 elevated events as blind labeling sheets, one per person. The sheets omit the pipeline's label and features and include a satellite-view link:

```bash
.venv/bin/python analysis/label_events.py export --output data/out/batch --config w10_q120_r7 --out data/labels/2020 --n 300 --labelers vraj,brandon
```

Each person fills in `human_label` (wildfire, local or uncertain) on their own, following `INSTRUCTIONS.md`. Then score:

```bash
.venv/bin/python analysis/label_events.py score --dir data/labels/2020 --output data/out/stream --configs w10_wm5_q120_c60_r7
```

This reports agreement between labelers (Cohen's kappa), uses the events you both agree on as ground truth, and gives each config's accuracy, per-class precision and recall, a confusion matrix, and accuracy split into dense vs. sparse sensor areas.

## Figures

Turn the sweep results into the paper's figures:

```bash
.venv/bin/python analysis/plot_frontier.py --results data/out/sweep.csv --out figures
```

This writes, as PNG and PDF (fonts embedded, ready for a journal):
- `frontier_rmse_ugm3`: decision latency against error for every setting. One color and marker per season (up to three, the colorblind-safe limit for scatter plots), with the Pareto frontier drawn as a line. Choose another error measure with `--metric`, for example `label_disagreement` or `smoke_alert_miss`.
- `knob_effects`: small multiples, each knob varied alone, one row per metric.
- `frontier_points.csv` / `.md`: every frontier point as a table, for the paper and for readers who cannot rely on color.

## Real monitors and context (free, no API keys)

```bash
.venv/bin/python history/download_context.py all --season 2020
```

This downloads EPA AQS hourly PM2.5, NASA FIRMS VIIRS fire detections, NOAA HMS smoke plumes and airport wind for the season, caches the raw files in `data/raw/`, and writes them to `data/history/<season>/` in the pipeline's formats:
- `monitors/`: ground truth, never read by the pipelines
- `context/fires`, `context/plume_cells`, `context/wind`: what the pipelines join against

The EPA server is slow (about 100 KB/s), so the first run takes 10 to 20 minutes per season. Assumptions (plume persistence and publication delays) are listed at the top of the script.

A summary of each season, plus a timeline figure:

```bash
.venv/bin/python analysis/season_overview.py --seasons 2020,2021,2024 --plot 2020 --compact
```

## Kafka streaming run (RQ4)

Kafka 4 from Homebrew runs without ZooKeeper. Start a broker in its own terminal:

```bash
/opt/homebrew/opt/kafka/bin/kafka-server-start /opt/homebrew/etc/kafka/server.properties
```

```bash
/opt/homebrew/opt/kafka/bin/kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic purpleair --partitions 4
```

Start the streaming job, then replay at a fixed speed with `--meta`, and measure:

```bash
.venv/bin/python spark/streaming_job.py --source kafka --topic purpleair --context data/history/2020/context --out data/out/kafka
```

```bash
.venv/bin/python replay/replay.py --history H.csv --sensors S.csv --sink kafka --topic purpleair --speed 300 --meta data/out/kafka_meta.json
```

```bash
.venv/bin/python analysis/measure_streaming.py --output data/out/kafka --config w10_wm5_q120_c60_r7 --meta data/out/kafka_meta.json
```

The first Kafka run downloads Spark's Kafka connector from Maven.

**Hadoop note:** if `HADOOP_CONF_DIR` points at a configuration with `fs.defaultFS=hdfs://...`, Spark would send every path to HDFS. SmokeStream pins the local file system; set `SMOKESTREAM_FS=hdfs://localhost:9000` to use HDFS instead.

## Historical PurpleAir data

History costs points per row and per field: PurpleAir's own example is 531,000 points for 10 sensors over one year. The downloader therefore estimates first, never spends past `--max-points`, and resumes where it stopped. It defaults to a 60-sensor sample in the Bay Area, because nearby sensors matter more for hexagon comparisons and neighbour checks than wide coverage.

1. Pick sensors that were active during the season. This is one cheap call, and you can add `--start`/`--end` to cover just a peak smoke stretch:

```bash
.venv/bin/python history/download_purpleair.py sensors --season 2020 --sample 60
```

2. See what the download would cost. This spends nothing:

```bash
.venv/bin/python history/download_purpleair.py estimate --season 2020 --average 10
```

3. Measure the real price once. Make one request, check its cost on develop.purpleair.com, and divide by the rows it printed:

```bash
.venv/bin/python history/download_purpleair.py download --season 2020 --max-points 50000 --max-requests 1
```

4. Download within a budget, passing your measured `--points-per-row`. Rerunning later resumes where it stopped:

```bash
.venv/bin/python history/download_purpleair.py download --season 2020 --max-points 900000 --points-per-row 6
```

5. Merge into the files the replay tool and the batch job read:

```bash
.venv/bin/python history/download_purpleair.py merge --season 2020
```

The output is `data/history/2020/history_avg10.csv` and `data/history/2020/sensors.csv`. Seasons are 2020 (Aug 1 to Sep 30), 2021 and 2024 (Jul 1 to Sep 15, both including the Fourth of July).

**Monitor-anchored sampling (recommended for the monitor evaluation):** a random sample leaves most monitors with no sensor nearby (in 2020, 27 random sensors covered only 5 of 20 monitors within 5 km). Pick the closest sensors around each monitor instead, keeping any already downloaded:

```bash
.venv/bin/python history/download_purpleair.py sensors --season 2020 --start 2020-08-16 --end 2020-09-16 --near-monitors data/history/2020/monitors/monitors.csv --per-monitor 1 --radius-km 3 --keep-downloaded
```

The candidate list is saved, so later re-sampling with `--reuse-candidates` costs no points.

**Resolution vs. cost:** `--average 10` (10-minute averages) is the default. `--average 0` gives the raw 2-minute data that 2-minute windows need, but costs about 5 times more. PurpleAir's terms do not allow redistributing the raw data, so the published benchmark should share code, sensor IDs, time ranges and our labels, not the readings themselves.

## Methodology notes (these matter for the paper)

- **One micro-batch per minute of simulated time.** Spark judges lateness once per micro-batch, so if a batch spans hours of data, late readings slip through and the late-data setting appears to do nothing. `replay.py --file-seconds 60` writes one file per simulated minute, and the streaming job reads one file per batch. This also makes runs deterministic.
- **Simulator instead of hours of streaming.** At about 0.27 s per micro-batch, a 60-day season in 1-minute batches takes about 6.5 hours per setting. `common/simulate.py` reproduces the streaming job's output in one batch pass. `tests/test_simulate.py` runs both on the same replay and checks that every window, reading count and PM2.5 value matches.
- **Spark's late-data rule (found by that test).** Spark drops a reading only if its window's end is at or before the *previous* batch's watermark, not the current one. That makes it slightly more lenient than the documentation suggests. The simulator follows the engine's actual behavior.
- **EPA correction input.** We use the `cf_1` channels, as in the papers we cite. Some tools use `cf_atm`. The Milestone 2 check (our corrected values vs. EPA-corrected values) settles this on real data.
- **2024 AQI breakpoints** (Good ends at 9.0 µg/m³) are used throughout.
- **Averaged history arrives after its interval.** A 10-minute average stamped 20:00 covers 20:00 to 20:10, so the replay adds the interval to every arrival (`--interval-seconds 600`; `run_season.py` sets it).
- **Context is judged at decision time.** A fire detection, plume or wind reading counts only if it was published and picked up by an Airflow refresh by `window_end + watermark`. The hindsight baseline sees all context. This is how the context-freshness knob takes effect, and the simulator and streaming job use the same cutoff, so they still match exactly.
- **The neighbour check is causal.** A sensor is excluded only after being off from its neighbours for most of the last `qc_history` minutes, using past windows only, in streaming and batch alike. Short history removes faulty sensors fast but also hides real local spikes and the leading edge of a smoke front.
- **Measuring the QC knob needs monitors.** Comparing a stream with a baseline that uses the same QC setting cancels out the QC effect itself, so its real effect is measured against regulatory monitors (`run_sweep.py --monitors`). On synthetic data a 30-minute history beats a 6-hour one, because it drops a drifting sensor sooner.
- **Missing values stay missing.** A hexagon whose sensors were all excluded has no PM2.5, no AQI and no alert. An earlier version turned a missing value into AQI 500 ("Hazardous"); a regression test now guards against that.
- **Reading the frontier across window sizes.** Each point's error is measured against the hindsight baseline at the same window size, so it shows what real-time processing costs at that resolution. A 60-minute window can score well on that measure while still being too coarse for warnings. Comparing window sizes on an absolute scale needs the monitor-based evaluation.
- **Known weakness of the labels (expected RQ3 finding).** Where sensors are sparse, the "extent" signal is missing. A local spike during fire season can then be blamed on an upwind fire. The synthetic test shows this with 12 sensors; with 30 it labels the same barbecue correctly.

## Still to build

- Run everything on real data (needs the recorder's delay profile, PurpleAir history and Brandon's context and monitor tasks)
- Hand-label the 300 events (you and Brandon)
- Airflow task bodies (see the contracts in `airflow/dags/smokestream_dags.py`)

## Live recorder

The recorder polls a fixed random sample of outdoor PurpleAir sensors in Northern California and saves each sensor's `last_seen` time next to our poll time. From that we measure how stale readings are when a real-time system would use them and how often sensors drop out. The replay tool later injects this delay profile into the historical fire seasons.

It uses only the Python standard library, so there is nothing to install.

### 1. Add your key

```bash
cp .env.example .env
```

Open `.env` and paste your PurpleAir READ key after `PURPLEAIR_READ_KEY=`.

### 2. Pick the sensor sample (once)

```bash
python3 recorder/recorder.py sample
```

This saves `data/live/sensors_sample.csv`. Do not re-run it during the week, or the sample changes.

### 3. Check the points cost with one poll

```bash
python3 recorder/recorder.py once
```

Then open develop.purpleair.com and see how many points that call used. The script prints how many calls a week takes. With the defaults (200 sensors, `last_seen` only, every 5 minutes) the week should fit inside the 1,000,000 free points, but confirm before starting.

### 4. Start the week-long run

Keep the laptop plugged in. `caffeinate` stops the Mac from idle-sleeping, and `nohup` keeps the recorder running after you close the terminal:

```bash
nohup caffeinate -i python3 recorder/recorder.py run > data/live/recorder.log 2>&1 &
```

Closing the lid still sleeps the Mac. Any sleep shows up as a gap in `data/live/polls.csv`, and the analysis excludes those gaps, but fewer gaps means better data.

### 5. Check progress any time

```bash
python3 recorder/recorder.py status
```

To stop it:

```bash
pkill -f "recorder.py run"
```

### Output files (`data/live/`)

| File | Contents |
|---|---|
| `sensors_sample.csv` | the sampled sensors and their locations |
| `readings_<profile>_YYYY-MM-DD.csv` | one row per sensor per poll: poll time, `last_seen`, `staleness_s` (poll time minus `last_seen`) |
| `polls.csv` | one row per poll: status, rows returned, request time, errors |
| `recorder.log` | console output when run with `nohup` |
