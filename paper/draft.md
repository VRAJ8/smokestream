# How Fast Can a Smoke Alert Be? Measuring the Latency-Accuracy Trade-off of Real-Time Wildfire Smoke Detection from Crowdsourced Sensor Streams

*Working draft. Sections marked [RESULTS] are filled in as experiments finish. Target venues: ACM DEBS, ACM SIGSPATIAL workshops, IEEE Big Data workshops.*

## Abstract

Low-cost PurpleAir sensors have become the main way many people in the western United States check wildfire smoke. The methods that turn their readings into numbers people trust, from the EPA correction to sensor quality checks, were developed and validated in hindsight, on hourly or daily averages computed after all the data had arrived. A real-time system cannot wait for that. It must decide while readings arrive late, sensors drop out, and fire and plume data may be hours old. We measure what deciding in real time costs. We build a streaming pipeline (Kafka, Spark Structured Streaming, MongoDB, Snowflake) that corrects and quality-checks sensor readings and uses fire, smoke-plume and wind context to separate wildfire smoke from local pollution, and a batch pipeline that runs identical logic with full hindsight. To sweep the design space at season scale, we introduce a batch simulator of Structured Streaming's watermark semantics and validate that it reproduces the engine's output exactly. Replaying three San Francisco Bay Area fire seasons (2020, 2021, 2024) and scoring every configuration against held-out regulatory monitors, we map the latency-accuracy frontier for smoke alerting across window size, late-data tolerance, quality-check history and context freshness. [RESULTS: one-sentence headline.]

## 1. Introduction

On September 9, 2020, the sky over the San Francisco Bay Area turned orange. Weeks earlier, lightning had started dozens of fires around the region, and a monitor in Santa Cruz County next to the CZU Lightning Complex recorded hourly PM2.5 above 800 µg/m³, more than fifteen times the threshold for "Unhealthy" air. For many residents the most immediate source of information was not the handful of regulatory monitors but the thousands of crowdsourced PurpleAir sensors, and the maps that correct and display them.

Those maps are real-time systems, and every real-time system makes design choices its users never see: how long to average readings, how long to wait for late data, how much history a sensor needs before it is trusted, and how stale fire and plume data may be. Each choice trades speed for accuracy. For a data engineer these are the familiar controls of stream processing: windows, watermarks and state [Akidau et al. 2015]. For the public they decide whether an alert arrives early enough to act on, or whether it is a false alarm that teaches people to ignore the next one. The correction and quality-control methods behind these maps were validated in batch, with complete data [Barkjohn et al. 2021, 2022]; to our knowledge nobody has measured what processing in real time costs.

This paper makes four contributions:

1. **A measurement methodology** for the cost of real-time processing in sensor-network alerting: a delay-aware replay of historical data, a streaming pipeline and a batch pipeline that share all transformation code, and a held-out evaluation against regulatory monitors that reports accuracy, alert reliability and lead time over official values.
2. **A validated simulator of Structured Streaming's watermark semantics.** Replaying a season through the real engine in fine micro-batches takes hours per configuration. Our simulator reproduces the engine's output exactly, window for window, in seconds, which makes season-scale design sweeps practical. Building and validating it exposed two pitfalls that silently invalidate naive experiments (Section 4.3).
3. **The latency-accuracy frontier for smoke alerting** across four streaming knobs and three fire seasons. [RESULTS]
4. **An analysis of in-stream smoke attribution** (wildfire vs. local) using fire, plume and wind context, including where it fails: in a heavy season, plume overlap is almost always true, and in sparse sensor areas spatial extent is unavailable. [RESULTS]

## 2. Background and Related Work

**Wildfire smoke exposure.** Smoke exposure harms respiratory health [Reid et al. 2016], has risen sharply across the United States [Burke et al. 2021], and has reversed much of the improvement in PM2.5 in western states [Burke et al. 2023]. Machine-learning models estimate daily smoke PM2.5 on a 10 km grid [Childs et al. 2022], but they are retrospective and daily.

**Correcting low-cost sensors.** PurpleAir sensors overestimate PM2.5, especially in smoke. Barkjohn et al. developed the U.S.-wide correction used on the AirNow Fire and Smoke Map, reducing error from about 8 to 3 µg/m³ [2021], and extended it to extreme smoke [2022]. Crowdsourced PurpleAir data has also shown how smoke penetrates indoors [Liang et al. 2021]. All of this work evaluates corrections on complete, after-the-fact data.

**Public smoke maps.** The AirNow Fire and Smoke Map shows corrected sensor data alongside satellite fire detections [Schroeder et al. 2014] and NOAA Hazard Mapping System plumes as separate layers. Its processing settings are fixed, and their accuracy cost is not reported.

**Stream processing.** Kafka [Kreps et al. 2011] and Spark Structured Streaming [Armbrust et al. 2018] are the standard open tools for fault-tolerant streaming. The Dataflow model frames stream processing as a balance between correctness, latency and cost, with windows, watermarks and triggers as the controls [Akidau et al. 2015]. Lambda and Kappa architectures describe how batch and streaming views coexist [Marz and Warren 2015; Kreps 2014]. Our work applies the Dataflow lens to a public-health alerting problem and measures the trade-off it describes, on real data, at season scale.

## 3. System

**Replay with realistic arrivals.** Historical PurpleAir readings are re-sent in arrival order. Each reading's arrival time is its event time plus a delay drawn from an empirical profile, and sensors drop out for stretches drawn from observed outage statistics. The profile is measured by polling a fixed random sample of live sensors for a week and recording, at each poll, how old each sensor's latest reading is. The replay writes to Kafka at a chosen speed, or to one file per simulated minute (Section 4.3).

**Shared transformation code.** Both pipelines run the same code. Readings are validated (range checks; the two laser channels must agree within 5 µg/m³ or 70%), corrected with the EPA extended U.S.-wide equation, mapped to H3 hexagons at resolution 7 (about 5 km²), and averaged per sensor in tumbling event-time windows. The streaming job applies a watermark; the batch job sees every reading.

**Neighbour quality check.** A sensor is deviant in a window when it differs from the median of other sensors within two hexagon rings by more than 10 µg/m³ and 100%. It is excluded only when it was deviant in at least 75% of its windows over the last *QC history* minutes. Short history removes faulty sensors quickly but can also suppress real local spikes and the leading edge of a smoke front.

**Context and labels.** Each elevated hexagon window (AQI above 100) receives four signals: *extent*, the share of hexagons within three rings that are also elevated; the PM1/PM2.5 ratio; whether it lies inside an HMS smoke plume; and whether an active fire lies upwind (within 150 km, within 45° of the downwind direction, or within 50 km in calm air). Context counts only if it had been published and picked up by a refresh at the configured interval before decision time, defined as the window end plus the watermark. A rule-based baseline labels each window wildfire, local or uncertain, and a smoke alert fires when AQI exceeds 150 and the window is not labeled local.

**Storage and orchestration.** Finalized windows are appended to Parquet and upserted into a MongoDB collection with geospatial and TTL indexes for live "near me" queries. A Snowflake star schema with a Type 2 slowly changing sensor dimension holds the history and all pipeline outputs side by side, so every streaming-versus-hindsight comparison is a self-join. Airflow orchestrates the slower sources (fires, plumes, wind, monitors) and historical backfills.

## 4. Methodology

### 4.1 Research questions

RQ1: How much accuracy and alert reliability does streaming lose compared with full hindsight on the same data? RQ2: How do window size, late-data tolerance, QC history and context freshness trade latency against accuracy? RQ3: Can in-stream context separate wildfire smoke from local spikes and reduce false alarms? RQ4: Can the pipeline sustain accelerated replay within a latency target on commodity hardware?

### 4.2 Data

For each season (August 1 to September 30, 2020; July 1 to September 15, 2021 and 2024; Bay Area, 36.9-38.3° N, 123.0-121.2° W) we use: PurpleAir 10-minute history for a random sample of outdoor sensors active during the season; EPA AQS hourly PM2.5 at regulatory monitors, used only as held-out ground truth; NASA FIRMS VIIRS 375 m vegetation-fire detections within about 150 km; NOAA HMS smoke polygons, clipped to the region and converted to H3 cells; and wind from ASOS airport stations. Table 1 summarizes the seasons. [RESULTS: final table.]

### 4.3 A validated simulator of watermark semantics

Running the streaming engine over a 60-day season in 1-minute micro-batches took about 0.27 s per batch on our laptop, or 6.5 hours per configuration, which rules out a sweep of dozens of configurations across three seasons. We therefore simulate the engine. Records are grouped into the same arrival slices the replay produces. For each slice we compute the running maximum event time; a window is finalized in the first batch whose eviction watermark (the maximum event time seen in earlier batches, minus the tolerance) reaches its end. A reading in batch *k* is dropped only if its window ended at or before the watermark of batch *k-1*.

That last rule was not our first guess. We initially used the current batch's watermark, and an automated equivalence test, which runs the real engine and the simulator on the same replay and compares every window, failed: the engine counted readings the simulator had dropped. Spark (since 3.4) filters late rows with the previous batch's watermark and evicts state with the current one. With the corrected rule the outputs match exactly for every configuration we test, including neighbour exclusions carried across batches, context features and labels.

A second pitfall concerns batch granularity. Spark evaluates lateness once per micro-batch, so if each batch spans hours of data, no reading is ever late and every late-data setting produces identical output. We observed exactly this before switching to fixed 1-minute slices of simulated time. Any study that replays data into a streaming engine to measure lateness effects must control batch granularity in simulated time, not wall-clock time.

### 4.4 Evaluation

Every configuration is compared both with the hindsight baseline at the same window size and with held-out regulatory monitors. For each monitor and hour we estimate PM2.5 from the hexagons within 5 km (inverse-distance weighted) and report RMSE, MAE, bias and R². Unhealthy hours (PM2.5 above 55.4 µg/m³) give hourly alert precision, recall and false-alarm rate, both for value-only alerts and for smoke alerts that ignore windows dominated by local labels. For each Unhealthy episode, lead time is the difference between when the stream first alerted nearby and when the official hourly value would have been published (the end of the hour plus 30 minutes). For RQ3, two people independently label a stratified sample of 300 elevated events as wildfire, local or uncertain, blind to the pipeline's labels; we report Cohen's kappa and score each configuration against the events both labelers agree on, split by sensor density.

### 4.5 Experiment design

Knob levels: window 10, 30 and 60 minutes; late-data tolerance 0, 2, 5, 15 and 30 minutes; QC history 30, 120 and 360 minutes; context refresh 15, 60 and 360 minutes. Each knob is varied around the default (10, 5, 120, 60), and late-data tolerance and QC history are crossed in a full grid. Because the delay profile shapes every result, we repeat the sweep under the measured profile and under faster and slower sensitivity profiles.

## 5. Results

[RESULTS]

5.1 Cost of real time (RQ1). 5.2 The frontier (RQ2): Figure, frontier table. 5.3 Smoke vs. local (RQ3): kappa, accuracy by density, effect on false alarms. 5.4 Performance (RQ4): sustained throughput 641 messages/s at 300x real time with 95th-percentile processing latency of 11.5 s on one laptop (preliminary, synthetic load); backlog drain at about 14,500 messages/s.

## 6. Discussion and Threats to Validity

*Delay profile.* Results depend on how late readings arrive; we measure one week of live data and report sensitivity to faster and slower profiles. *Context timing.* Archived fire and plume records lack publication times; we use typical delays and test sensitivity. *Ground truth.* Regulatory monitors are sparse and hourly; sub-hourly accuracy cannot be scored against them, and monitors near fires (such as the 826 µg/m³ Santa Cruz County site) dominate squared-error metrics, so we also report per-site results. *Sample.* Points-based API pricing limits the PurpleAir sample to about 60 sensors per season at 10-minute resolution. *Labels.* The baseline labeling rule is deliberately simple and its thresholds are not tuned; RQ3 measures where it fails rather than proposing a classifier. *Simulator scope.* The simulator reproduces Spark's semantics for the file-sliced replay; the real engine remains the reference for performance (RQ4).

## 7. Conclusion

[RESULTS]

## Data and code availability

Code, replay tool, delay profile, sensor IDs, time ranges and the hand-labeled events will be released. PurpleAir's terms do not allow redistributing raw readings, so others re-download them with their own key; all other data is public.

## References

(Same as the proposal, plus: Akidau et al. 2015; Barkjohn et al. 2022; Armbrust et al. 2018.)
