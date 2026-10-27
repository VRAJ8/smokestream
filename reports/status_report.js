const fs = require('fs');
const { P, H, BUL, TABLE, FIG, header, build } = require('./lib');

const [, , dataPath, figPath, out] = process.argv;
const d = JSON.parse(fs.readFileSync(dataPath, 'utf8'));
const k = d.kafka;
const fmt = (n) => Number(n).toLocaleString('en-US');
const seasonRow = (s) => s.missing
  ? [s.season, 'downloading', '', '', '', '', '', '']
  : [s.season, `${s.monitor_sites}`, fmt(s.unhealthy_hours), fmt(s.unhealthy_episodes), `${s.days_with_unhealthy}`,
     `${Math.round(s.peak_pm25)}`, `${s.plume_days}`, fmt(s.fire_detections)];
const s2020 = d.seasons.find((s) => s.season === '2020');

const body = [
  ...header('SmokeStream: Intermediate Status Report',
    'Measuring the Latency-Accuracy Trade-off in Real-Time Wildfire Smoke Detection  |  DATA 226, San José State University, Fall 2026',
    'Team: Vraj Patel and Brandon Moriguchi'),

  H('1. Progress Toward the Goal'),
  P('The pipeline described in the proposal is built and tested end to end, and real ground-truth and context data for all three fire seasons has been downloaded and processed. The one input still missing is the PurpleAir sensor history itself (Section 3).'),
  BUL('**Streaming pipeline.** A replay tool feeds historical sensor data into Kafka with realistic arrival delays and outages. Spark Structured Streaming validates and EPA-corrects each reading, maps it to an H3 hexagon, averages in event-time windows under a watermark, runs a neighbour quality check, joins fire, plume and wind context, labels elevated hexagons as wildfire, local or uncertain, and writes to Parquet, with an upsert path to a MongoDB live store (to be connected to Atlas). A batch job runs identical logic with full hindsight.'),
  BUL('**Watermark simulator.** The real engine needs about 6.5 hours per setting to replay a season in 1-minute micro-batches, too slow for the sweep. Our batch simulator of Spark\'s watermark semantics gives the same output in seconds; an automated test requires every window, count, exclusion and label to match the engine exactly, and it does.'),
  BUL('**Real data for all three seasons** from free public sources (Table 1): EPA AQS hourly PM2.5 at regulatory monitors (ground truth), NASA FIRMS VIIRS fire detections, NOAA HMS smoke plumes converted to H3 cells, and airport (ASOS) wind.'),
  BUL(`**Evaluation and storage.** Held-out evaluation against regulatory monitors (accuracy, alert precision and recall, lead time over the official value), a blind labeling tool with Cohen\'s kappa for RQ3, a sweep runner and paper-ready figures. Snowflake star schema (SCD Type 2 sensor dimension), MongoDB indexes and Airflow DAGs are defined; the backfill DAG runs the downloader. ${d.tests} automated tests; code in git.`),

  H('2. Findings So Far'),
  ...TABLE([
    ['Season', 'Monitor sites', 'Unhealthy site-hours', 'Episodes', 'Days with Unhealthy air', 'Peak PM2.5 (µg/m³)', 'Plume days', 'Fire detections'],
    ...d.seasons.map(seasonRow),
  ], [0.09, 0.11, 0.14, 0.1, 0.15, 0.15, 0.11, 0.15],
  'Table 1. The three study seasons in the real data (Bay Area). Unhealthy = hourly PM2.5 above 55.4 µg/m³ (AQI 150). Plume day = HMS smoke over at least half of the region. Fire detections within about 150 km.'),
  ...FIG(figPath, 5.3, `Figure 1. The 2020 season. Monitor PM2.5 (median and range across ${s2020.monitor_sites} sites; the ${Math.round(s2020.peak_pm25)} µg/m³ peak is a Santa Cruz County monitor beside the CZU Lightning Complex fire), share of the region under an HMS plume, and daily VIIRS fire detections.`),
  BUL(`**Plenty of events to evaluate.** 2020 alone has ${fmt(s2020.unhealthy_hours)} Unhealthy monitor-hours in ${fmt(s2020.unhealthy_episodes)} episodes. ${d.season_note}`),
  BUL(`**Plume overlap alone cannot tell smoke from local pollution.** HMS plumes covered more than half of the region on ${s2020.plume_days} of 61 days in 2020, and on ${d.seasons[2].plume_days} days in 2024, a season with almost no wildfire smoke at ground level: plumes often mark smoke aloft. Labels will have to lean on spatial extent and upwind fires.`),
  BUL(`**Streaming performance (RQ4, preliminary, synthetic load).** On one laptop, Kafka and Spark sustained ${Math.round(k.throughput_msgs_per_s)} messages/s at ${k.speed}x real time with a 95th-percentile processing latency of ${k.latency_p95_s.toFixed(1)} s (99th: ${k.latency_p99_s.toFixed(1)} s), well under the 60 s target. Draining a backlog ran at about ${fmt(d.capacity)} messages/s, roughly eight times the load of 2,000 sensors reporting every 2 minutes replayed at 100x.`),
  BUL('**Two pitfalls that silently invalidate naive experiments.** Spark judges lateness once per micro-batch, so replaying in large batches makes every late-data setting look identical; we replay in fixed 1-minute slices of simulated time. And Spark drops a late reading only if its window closed before the *previous* batch\'s watermark, more leniently than its documentation suggests; our validation test found this, and the simulator follows it.'),
  BUL('**Synthetic check of the measurement chain (not a result).** With known ground truth, the EPA correction cut error against monitors from 26.5 to 2.9 µg/m³, and raising late-data tolerance from 0 to 30 minutes cut warning lead from 69 to 34 minutes.'),

  H('3. Difficulties and How We Plan to Resolve Them'),
  BUL('**PurpleAir history is expensive.** The API charges points per row and per field (PurpleAir\'s own example is 531,000 points for 10 sensors over a year), so three full seasons at 10-minute resolution for 60 sensors would cost roughly $80, and 2-minute data about five times more. We will download 10-minute data for the peak smoke weeks of each season within the free allowance and a small budget, and we are requesting research points from PurpleAir. The smallest window setting becomes 10 minutes.'),
  BUL('**The live delay profile is not recorded yet.** The week of live recording that calibrates arrival delays starts this week; we will also run the sweep under faster and slower profiles and report sensitivity.'),
  BUL('**Archived context lacks publication times,** so we use typical delays (about 3 hours for FIRMS, 2 hours after the HMS analysis) and will test sensitivity to them.'),
  BUL('**Laptop limits.** A full-season run first ran out of memory because Spark recomputed shared intermediate tables; materializing them halved run time. A local Hadoop setup also redirected paths to HDFS; the pipeline now pins the local file system (HDFS optional).'),

  H('4. Remaining Tasks'),
  ...TABLE([
    ['Dates', 'Task'],
    ['Oct 28 - Nov 3', 'Start the live recorder; download PurpleAir 2020 peak weeks; build the delay profile'],
    ['Nov 4 - Nov 10', 'Real-data runs for 2020: knob sweep, monitor evaluation, first latency-accuracy frontier (mid-term check)'],
    ['Nov 11 - Nov 20', '2021 and 2024 seasons; delay and context sensitivity; MongoDB and Snowflake loads on real outputs; Airflow runs'],
    ['Nov 18 - Nov 25', 'Hand-label 300 events with two independent labelers and score RQ3'],
    ['Nov 25 - Dec 7', 'Final report, presentation and paper draft (targets: ACM DEBS, ACM SIGSPATIAL workshops)'],
  ], [0.17, 0.83]),

  H('5. Other'),
  P('**Reproducibility.** Every step runs on one laptop with documented commands, and a synthetic generator with known ground truth tests the full chain without API keys. **Use of Generative AI and other sources:** [To be completed by the team.]'),
];

build(out, 'SmokeStream: Intermediate Status Report', body);
