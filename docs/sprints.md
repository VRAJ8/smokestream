# SmokeStream sprint log (one-week sprints)

Built from dated artifacts (the proposal and status-report files, file creation dates and the git history) and the author's own notes on exploration work that left no file behind. The Evidence column says which is which.

| Sprint (week) | Goal | Done | Evidence |
|---|---|---|---|
| 1. Sep 28 to Oct 4 | Choose a topic, write the proposal | Brainstormed and explored several candidate areas; narrowed to two topics and drafted both proposals (GreenPipe, SmokeStream); SmokeStream proposal submitted Oct 1 | `SmokeStream_Proposal.pdf`, `GreenPipe_Proposal.docx` (not in repo) |
| 2. Oct 5 to Oct 11 | Pipeline skeleton end to end on synthetic data | Replay tool, Spark streaming and batch jobs, watermark simulator, enrichment, monitor evaluation, labeling tool, sweep runner, synthetic generator, tests | File creation dates (Oct 8) of `spark/`, `common/`, `analysis/`, `tests/` |
| 3. Oct 12 to Oct 18 | Learn the tools and the domain | Tried the candidate frameworks and data sources on sample data; read the sensor-correction and stream-processing papers that set the direction (Section 2 of the report) | Author's notes |
| 4. Oct 19 to Oct 25 | Settle the stack and the data plan | Compared data sources (PurpleAir API pricing, EPA AQS, FIRMS, HMS, ASOS) and how to run real data within budget; outcomes appear in the Oct 27 commits | Author's notes; commits of Oct 27 |
| 5. Oct 26 to Nov 1 | Real data and the mid-project status report | Public monitors, fires, plumes, wind for 3 seasons; PurpleAir 2020 history (40 sensors); correction validated against monitors; live recorder started; Kafka throughput measured; MongoDB Atlas live store and demo; Snowflake loader; status report submitted | Commits of Oct 27 and 28; `SmokeStream_Status_Report.pdf` |
| 6. Nov 2 to Nov 8 | (no dated artifact) | | |
| 7. Nov 9 to Nov 15 | Orchestration and more data | Measured delay profile (207 polls) and final 2020 rerun; Airflow 3 with live FIRMS, HMS, wind and AirNow feeds; event windows for July 4 2021 and 2024 and the 2021 smoke | Commit of Nov 10 |
| 8. Nov 16 to Nov 17 | Course techniques and final deliverables | Bloom, Flajolet-Martin, DGIM, reservoir sampling; LSH; k-anonymity and differential privacy; data-poisoning test; Snowflake load and verification; IEEE LaTeX report; slides; public GitHub | Commits of Nov 16 and 17 |

## Exploration behind the commits (author's notes)

- **Topic.** Several areas were brainstormed and explored before the latency-accuracy question for smoke alerts was chosen; a second full proposal (GreenPipe) was drafted along the way.
- **Tools.** Frameworks and services were tried hands-on before settling the stack: orchestration (Prefect and Dagster from the homework, then Airflow), Kafka and Spark Structured Streaming, MongoDB Atlas, Snowflake, H3.
- **Data.** Sample data was run end to end to see what results looked like before spending PurpleAir points on real history.
- **Literature.** Related papers on sensor correction, wildfire smoke exposure and stream-processing semantics were read to sharpen the research questions and the method (cited in Section 2 of the report).

## Planned vs. done (from the status report's dated plan, Oct 27)

| Planned | Status at submission |
|---|---|
| Oct 28 to Nov 3: finish live recording; rerun 2020 with the full delay profile; request PurpleAir research points | Recording stopped after about 18 hours of polls (laptop sleep); profile built from 207 polls and 2020 rerun on Nov 10 |
| Nov 4 to Nov 10: 2021 and 2024 downloads and runs; frontier and monitor evaluation | Done Nov 10 as three 4-day event windows, within the free points |
| Nov 11 to Nov 20: delay and context sensitivity; Snowflake loads; Airflow runs | Context sensitivity in every sweep; Airflow DAGs run; Snowflake loaded Nov 17 (6.28 million rows, SQL matches Python on all 65 settings); delay-profile sensitivity not run |
| Nov 18 to Nov 25: hand-label events with two labelers; score RQ3 | In progress |
| Nov 25 to Dec 7: final report, presentation, paper | Report and slides submitted Nov 17 |

## Backlog still open

- Score RQ3 with two independent labelers (Cohen's kappa)
- Delay-profile sensitivity runs (faster and slower profiles)
- Denser sensor samples for 2021 and 2024
- Sybil-resistant quality check (sensor age, owner limits, satellite cross-check)
