#!/usr/bin/env python3
"""RQ3 hand-labeling: export smoke events for blind labeling, then score the labels.

export  Finds elevated events (runs of consecutive hexagon windows with AQI > 100)
        in one pipeline output, draws a sample stratified by the pipeline's label,
        and writes one labeling sheet per person. Sheets are blind: they show
        when, where and how bad, plus a satellite-view link, but not the pipeline's
        label or the features it used. The pipeline labels go to events_key.csv.

score   Reads the filled-in sheets, reports agreement between labelers (Cohen's
        kappa), takes the events where all labelers agree as ground truth, and
        reports each pipeline config's accuracy against it: overall, per class, as
        a confusion matrix, and split by sensor density (where the "extent" signal
        exists or not).

Usage:
  python analysis/label_events.py export --output data/out/batch --config w10_q120_r7 \\
      --out data/labels/2020 --n 300 --labelers vraj,brandon
  python analysis/label_events.py score --dir data/labels/2020 --output data/out/stream \\
      --configs w10_wm5_q120_c60_r7,w10_wm15_q120_c60_r7
"""

import argparse
import random
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import h3  # noqa: E402
import pandas as pd  # noqa: E402

from common.evaluate import load_output  # noqa: E402

LABELS = ["wildfire", "local", "uncertain"]
ELEVATED_AQI = 100

INSTRUCTIONS = """# Labeling smoke events

Each row is a stretch of time when one hexagon (about 5 km2) had elevated PM2.5
(AQI above 100). Decide what caused it, on your own, without discussing rows with
the other labeler until both sheets are done.

Fill in `human_label` with one of:
- `wildfire`: wildfire smoke (regional haze, a plume visible on the satellite view,
  a known fire upwind, many nearby areas affected at once)
- `local`: a local source (one spot only, short, e.g. a barbecue, fireworks, a
  fireplace, construction dust)
- `uncertain`: the evidence does not settle it

`confidence`: 1 (guess), 2 (fairly sure), 3 (sure). `notes`: anything useful.

Evidence you can use: the satellite_view link (NASA Worldview; if it opens on the
wrong date, set the date in the timeline), news reports of fires, July 4 and other
holidays, and the time of day. Do not look at the pipeline outputs or events_key.csv.
"""


def find_events(out: pd.DataFrame, gap_windows: int = 1) -> pd.DataFrame:
    """Runs of elevated windows per hexagon; up to gap_windows missing windows are bridged."""
    elev = out[out["aqi"] > ELEVATED_AQI].sort_values(["h3", "window_start_s"])
    rows = []
    for cell, g in elev.groupby("h3"):
        current = []
        for r in g.itertuples(index=False):
            length = r.window_end_s - r.window_start_s
            if current and r.window_start_s - current[-1].window_end_s > gap_windows * length:
                rows.append(summarize(cell, current))
                current = []
            current.append(r)
        if current:
            rows.append(summarize(cell, current))
    return pd.DataFrame(rows)


def summarize(cell, windows):
    labels = Counter(w.smoke_label for w in windows if isinstance(w.smoke_label, str))
    top = labels.most_common()
    label = top[0][0] if top and (len(top) == 1 or top[0][1] > top[1][1]) else "uncertain"
    lat, lon = h3.cell_to_latlng(cell)
    extent_known = sum(1 for w in windows if pd.notna(w.extent)) / len(windows)
    start, end = windows[0].window_start_s, windows[-1].window_end_s
    return {
        "event_id": f"{cell}-{start}", "h3": cell, "start_s": start, "end_s": end,
        "duration_min": (end - start) // 60, "n_windows": len(windows),
        "latitude": round(lat, 3), "longitude": round(lon, 3),
        "peak_pm25": round(max(w.pm25 for w in windows), 1), "peak_aqi": max(int(w.aqi) for w in windows),
        "pipeline_label": label, "extent_known_share": round(extent_known, 2),
        "density": "dense" if extent_known >= 0.5 else "sparse",
        "in_plume_share": round(sum(bool(w.in_plume) for w in windows) / len(windows), 2),
        "upwind_fire_share": round(sum(bool(w.upwind_fire) for w in windows) / len(windows), 2),
    }


def stratified_sample(events: pd.DataFrame, n: int, seed: int) -> pd.DataFrame:
    """Up to n events, as even as possible across pipeline labels (rare classes kept whole)."""
    rng = random.Random(seed)
    groups = {lbl: g.sample(frac=1, random_state=seed) for lbl, g in events.groupby("pipeline_label")}
    chosen, remaining = [], n
    for i, lbl in enumerate(sorted(groups, key=lambda k: len(groups[k]))):
        take = min(len(groups[lbl]), remaining // (len(groups) - i))
        chosen.append(groups[lbl].head(take))
        remaining -= take
    sample = pd.concat(chosen) if chosen else events.head(0)
    order = list(range(len(sample)))
    rng.shuffle(order)  # mix the classes so the sheet's order gives nothing away
    return sample.iloc[order].reset_index(drop=True)


def satellite_link(lat, lon, start_s):
    day = pd.to_datetime(start_s, unit="s", utc=True).strftime("%Y-%m-%d")
    return (f"https://worldview.earthdata.nasa.gov/?v={lon - 0.6:.2f},{lat - 0.45:.2f},"
            f"{lon + 0.6:.2f},{lat + 0.45:.2f}&t={day}")


def cmd_export(args):
    out = load_output(args.output, args.config)
    events = find_events(out)
    if events.empty:
        sys.exit("No elevated events in that output.")
    sample = stratified_sample(events, args.n, args.seed)
    dest = Path(args.out)
    dest.mkdir(parents=True, exist_ok=True)
    sample.to_csv(dest / "events_key.csv", index=False)

    local = lambda s: pd.to_datetime(s, unit="s", utc=True).tz_convert("America/Los_Angeles").strftime("%Y-%m-%d %H:%M")
    sheet = pd.DataFrame({
        "event_id": sample["event_id"], "start_local": sample["start_s"].map(local),
        "end_local": sample["end_s"].map(local), "duration_min": sample["duration_min"],
        "latitude": sample["latitude"], "longitude": sample["longitude"],
        "peak_pm25": sample["peak_pm25"], "peak_aqi": sample["peak_aqi"],
        "satellite_view": [satellite_link(a, o, s) for a, o, s in zip(sample["latitude"], sample["longitude"],
                                                                       sample["start_s"])],
        "human_label": "", "confidence": "", "notes": "",
    })
    for person in args.labelers.split(","):
        sheet.to_csv(dest / f"labels_{person.strip()}.csv", index=False)
    (dest / "INSTRUCTIONS.md").write_text(INSTRUCTIONS)
    counts = sample["pipeline_label"].value_counts().to_dict()
    print(f"{len(events)} elevated events found; sampled {len(sample)} {counts}.")
    print(f"Sheets for {args.labelers} written to {dest}/ (label blind; keep events_key.csv away from labelers).")


def cohen_kappa(a, b):
    n = len(a)
    if n == 0:
        return None
    po = sum(x == y for x, y in zip(a, b)) / n
    ca, cb = Counter(a), Counter(b)
    pe = sum(ca[k] * cb[k] for k in set(ca) | set(cb)) / (n * n)
    return (po - pe) / (1 - pe) if pe < 1 else 1.0


def pipeline_labels(out: pd.DataFrame, key: pd.DataFrame) -> pd.Series:
    """Label each keyed event with a config's own majority label over the same hexagon and time."""
    labels = {}
    for ev in key.itertuples(index=False):
        w = out[(out["h3"] == ev.h3) & (out["window_start_s"] >= ev.start_s) & (out["window_end_s"] <= ev.end_s)]
        top = Counter(x for x in w["smoke_label"] if isinstance(x, str)).most_common()
        labels[ev.event_id] = (top[0][0] if top and (len(top) == 1 or top[0][1] > top[1][1])
                               else ("uncertain" if top else "missing"))
    return pd.Series(labels)


def cmd_score(args):
    d = Path(args.dir)
    key = pd.read_csv(d / "events_key.csv")
    sheets = {p.stem.removeprefix("labels_"): pd.read_csv(p, dtype=str).set_index("event_id")["human_label"]
              for p in sorted(d.glob("labels_*.csv"))}
    if len(sheets) < 2:
        sys.exit("Need at least two labeling sheets.")
    labels = pd.DataFrame(sheets).apply(lambda c: c.str.strip().str.lower())
    bad = labels.stack().loc[lambda v: ~v.isin(LABELS)]
    if not bad.empty:
        print(f"Ignoring {len(bad)} cells that are empty or not one of {LABELS}.")
    done = labels[labels.isin(LABELS).all(axis=1)]
    names = list(sheets)

    lines = [f"# RQ3 label evaluation ({len(done)} of {len(labels)} events labeled by everyone)", ""]
    for i, a in enumerate(names):
        for b in names[i + 1:]:
            k = cohen_kappa(list(done[a]), list(done[b]))
            agree = (done[a] == done[b]).mean() if len(done) else None
            lines.append(f"- Agreement {a} vs {b}: {agree:.1%}, Cohen's kappa {k:.3f}" if k is not None
                         else f"- Agreement {a} vs {b}: no events labeled by both")
    consensus = done[done.nunique(axis=1) == 1].iloc[:, 0].rename("truth")
    lines += [f"- Consensus (all labelers agree): {len(consensus)} events, used as ground truth", ""]

    rows = []
    for config_id in (args.configs.split(",") if args.configs else []):
        pred = pipeline_labels(load_output(args.output, config_id), key)
        joined = pd.concat([consensus, pred.rename("pred")], axis=1, join="inner").join(
            key.set_index("event_id")["density"])
        lines += [f"## {config_id}", ""]
        if joined.empty:
            lines += ["No consensus events to score.", ""]
            continue
        row = {"config_id": config_id, "events": len(joined), "accuracy": (joined["truth"] == joined["pred"]).mean()}
        for lbl in LABELS:
            tp = ((joined["pred"] == lbl) & (joined["truth"] == lbl)).sum()
            p_n, t_n = (joined["pred"] == lbl).sum(), (joined["truth"] == lbl).sum()
            row[f"{lbl}_precision"] = tp / p_n if p_n else None
            row[f"{lbl}_recall"] = tp / t_n if t_n else None
        for density, g in joined.groupby("density"):
            row[f"accuracy_{density}"] = (g["truth"] == g["pred"]).mean()
        rows.append(row)
        matrix = pd.crosstab(joined["truth"], joined["pred"], rownames=["consensus"], colnames=["pipeline"])
        lines += [f"Accuracy {row['accuracy']:.1%} on {len(joined)} events "
                  + ", ".join(f"({k.split('_', 1)[1]}: {v:.1%})" for k, v in row.items()
                              if k.startswith("accuracy_")), "", matrix.to_markdown(), ""]

    report = "\n".join(lines)
    (d / "report.md").write_text(report)
    if rows:
        pd.DataFrame(rows).to_csv(d / "label_scores.csv", index=False)
    print(report)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    e = sub.add_parser("export")
    e.add_argument("--output", required=True)
    e.add_argument("--config", required=True, help="usually the hindsight batch config")
    e.add_argument("--out", required=True)
    e.add_argument("--n", type=int, default=300)
    e.add_argument("--labelers", default="vraj,brandon")
    e.add_argument("--seed", type=int, default=226)
    e.set_defaults(func=cmd_export)
    s = sub.add_parser("score")
    s.add_argument("--dir", required=True)
    s.add_argument("--output", help="Parquet output folder with the configs to score")
    s.add_argument("--configs", help="comma-separated config ids to score against the consensus")
    s.set_defaults(func=cmd_score)
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
