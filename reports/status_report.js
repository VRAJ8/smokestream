const fs = require('fs');
const { P, H, BUL, TABLE, FIG, header, build } = require('./lib');

const [, , dataPath, figPath, out] = process.argv;
const d = JSON.parse(fs.readFileSync(dataPath, 'utf8'));
const k = d.kafka;
const L = d.live;
const Ls = d.live_split;
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
  P('The pipeline described in the proposal is built and tested end to end, real ground-truth and context data for all three seasons is processed, live PurpleAir recording started on October 27, and the first real PurpleAir history (40 sensors, Aug 16 to Sep 16, 2020; 143,748 ten-minute readings) has gone through the full experiment sweep.'),
  BUL('**Streaming pipeline.** A replay tool feeds historical sensor data into Kafka with realistic arrival delays and outages. Spark Structured Streaming validates and EPA-corrects each reading, maps it to an H3 hexagon, averages in event-time windows under a watermark, runs a neighbour quality check, joins fire, plume and wind context, labels elevated hexagons as wildfire, local or uncertain, and writes to Parquet and to a MongoDB Atlas live store (one document per hexagon, with geospatial "AQI near me" queries). A batch job runs identical logic with full hindsight.'),
  BUL('**Watermark simulator.** Replaying a season through the real engine in 1-minute micro-batches takes about 6.5 hours per setting. Our batch simulator of Spark\'s watermark semantics gives the same output in seconds, verified window for window against the engine by an automated test.'),
  BUL('**Real data** (Table 1): EPA AQS hourly PM2.5 at regulatory monitors (ground truth), NASA FIRMS VIIRS fires, NOAA HMS smoke plumes converted to H3 cells, airport (ASOS) wind, and PurpleAir history.'),
  BUL(`**Evaluation and storage.** Held-out evaluation against regulatory monitors (accuracy, alert precision and recall, lead time), a blind labeling tool with Cohen\'s kappa for RQ3 (300 events exported for 2020), a sweep runner and figures. Snowflake star schema with an SCD Type 2 sensor dimension and Airflow DAGs are defined. ${d.tests} automated tests; code in git.`),

  H('2. Findings So Far'),
  ...TABLE([
    ['Season', 'Monitor sites', 'Unhealthy site-hours', 'Episodes', 'Days with Unhealthy air', 'Peak PM2.5 (µg/m³)', 'Plume days', 'Fire detections'],
    ...d.seasons.map(seasonRow),
  ], [0.09, 0.11, 0.14, 0.1, 0.15, 0.15, 0.11, 0.15],
  'Table 1. The three study seasons in the real data (Bay Area). Unhealthy = hourly PM2.5 above 55.4 µg/m³ (AQI 150). Plume day = HMS smoke over at least half of the region. Fire detections within about 150 km.'),
  ...FIG(figPath, 4.7, `Figure 1. The 2020 season. Monitor PM2.5 (median and range across ${s2020.monitor_sites} sites; the ${Math.round(s2020.peak_pm25)} µg/m³ peak is a Santa Cruz County monitor beside the CZU Lightning Complex fire), share of the region under an HMS plume, and daily VIIRS fire detections.`),
  BUL(`**Plenty of events to evaluate.** 2020 alone has ${fmt(s2020.unhealthy_hours)} Unhealthy monitor-hours in ${fmt(s2020.unhealthy_episodes)} episodes. ${d.season_note}`),
  BUL(`**Plume overlap alone cannot tell smoke from local pollution.** HMS plumes covered more than half of the region on ${s2020.plume_days} of 61 days in 2020, and on ${d.seasons[2].plume_days} days in 2024, a season with almost no wildfire smoke at ground level: plumes often mark smoke aloft. Labels will have to lean on spatial extent and upwind fires.`),
  BUL(`**Streaming performance (RQ4).** On one laptop and synthetic load, Kafka and Spark sustained ${Math.round(k.throughput_msgs_per_s)} messages/s at ${k.speed}x real time with 95th-percentile processing latency of ${k.latency_p95_s.toFixed(1)} s, and drained a backlog at about ${fmt(d.capacity)} messages/s. On real data, replaying 14 hours of the September 10-11, 2020 smoke through Kafka, Spark and MongoDB Atlas at ${L.speed}x put every window in the live store with median latency of ${L.latency_p50_s.toFixed(0)} s (95th percentile ${L.latency_p95_s.toFixed(0)} s): ${Ls.designed_wait_wall_s_p50.toFixed(1)} s of it is the ${Math.round(Ls.designed_wait_sim_min_p50)} simulated minutes a window waits for later readings to pass the watermark, and the system itself adds about ${Math.round(Ls.overhead_s_p50)} s (5-second trigger, processing and the Atlas write).`),
  BUL('**Two pitfalls for streaming experiments.** Spark judges lateness once per micro-batch, so large replay batches make every late-data setting look identical (we replay in 1-minute slices of simulated time); and Spark drops late readings against the *previous* batch\'s watermark, more leniently than documented, which our validation test exposed.'),
  BUL('**The correction choice matters, and real data settles it.** Against 7,890 monitor-hours at 16 monitors, the EPA U.S.-wide linear correction on the cf_1 channel matched the monitors (bias -0.6 µg/m³, R² 0.83, sensor/monitor ratio 0.90 to 1.12 in every concentration band). A piecewise "extended" form found in public tools overestimated smoke-level concentrations by 46 to 67% with the same input, and uncorrected sensors read 1.7 to 2.1 times high. We use the linear form.'),
  BUL('**First real results (2020, preliminary).** With arrival delays measured from the live feed (median 85 s), late data costs little: the stream missed 2 to 2.5% of readings and, with 10-minute windows, differed from full hindsight by at most 0.15 µg/m³ (1.1 with 60-minute windows), about a hundred times less than the sensor-versus-monitor error (17.4 µg/m³). Stale context costs more: refreshing fire and plume data every 6 hours instead of every 15 minutes cut agreement with hindsight labels from 92% to 84%. Smoke alerts were 89% precise with a 2% false-alarm rate, caught 93% of the Unhealthy monitor-hours that had a sensor nearby, and came a median 89 to 109 minutes before the official hourly value (69 with 30-minute windows).'),

  H('3. Difficulties and How We Plan to Resolve Them'),
  BUL('**PurpleAir history is expensive.** The API charges points per row and field, so we use 10-minute data for each season\'s peak weeks (the smallest window is 10 minutes). The free allowance covered 40 sensors for the 2020 peak month; for 2021 we are requesting research points from PurpleAir.'),
  BUL('**Random sensors rarely sit near monitors.** In our first random sample only 5 of 20 monitors had a sensor within 5 km. Adding the closest sensor to each uncovered monitor raised that to 16 monitors and 53% of monitor-hours, and smoke-alert recall from 17% to 54%. Sparse sensors also rarely yield "local" labels (1 of the 300 sampled 2020 events), so RQ3 will lean on the July 4 events of 2021 and 2024.'),
  BUL('**Timing assumptions.** The delay profile rests on a few hours of live data so far (recording continues for about three days), and archived fire and plume records lack publication times, so we use typical delays (3 hours for FIRMS, 2 hours after the HMS analysis). We will report sensitivity to both.'),

  H('4. Remaining Tasks'),
  ...TABLE([
    ['Dates', 'Task'],
    ['Oct 28 - Nov 3', 'Finish live recording (about three days); rerun 2020 with the full delay profile; request PurpleAir research points'],
    ['Nov 4 - Nov 10', '2021 and 2024 downloads and runs; full latency-accuracy frontier and monitor evaluation (mid-term check)'],
    ['Nov 11 - Nov 20', 'Delay and context sensitivity; Snowflake loads on real outputs; Airflow runs'],
    ['Nov 18 - Nov 25', 'Hand-label 300 events with two independent labelers and score RQ3'],
    ['Nov 25 - Dec 7', 'Final report, presentation and paper draft (targets: ACM DEBS, ACM SIGSPATIAL workshops)'],
  ], [0.17, 0.83]),

  H('5. Other'),
  P('**Reproducibility.** Every step runs on one laptop with documented commands, and a synthetic generator with known ground truth tests the full chain without API keys. **Use of Generative AI and other sources:** [To be completed by the team.]'),
];

build(out, 'SmokeStream: Intermediate Status Report', body);
