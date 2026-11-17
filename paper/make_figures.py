#!/usr/bin/env python3
"""Figures for the report and slides, from the analysis outputs under data/out/.

Usage: python paper/make_figures.py   (writes paper/figures/*.pdf and *.png)
"""

import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import matplotlib  # noqa: E402

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import matplotlib.ticker  # noqa: E402
import pandas as pd  # noqa: E402

OUT = ROOT / "paper" / "figures"
OUT_DATA = ROOT / "data" / "out"
# Okabe-Ito, distinguishable with color-vision deficiencies; every series also gets its own marker
RUNS = [("2020-measured", "2020 season", "#0072B2", "o"), ("2021-smoke-measured", "2021 smoke", "#D55E00", "s"),
        ("2021-july4-measured", "2021 July 4", "#009E73", "^"), ("2024-july4-measured", "2024 July 4", "#CC79A7", "D")]

plt.rcParams.update({"font.size": 8, "axes.titlesize": 8, "axes.labelsize": 8, "legend.fontsize": 7,
                     "xtick.labelsize": 7, "ytick.labelsize": 7, "axes.spines.top": False,
                     "axes.spines.right": False, "savefig.bbox": "tight", "savefig.dpi": 300})


def sweeps():
    frames = [pd.read_csv(p) for p in OUT_DATA.glob("season_*/sweep.csv")]
    return pd.concat(frames)


def save(fig, name):
    for ext in ("pdf", "png"):
        fig.savefig(OUT / f"{name}.{ext}")
    plt.close(fig)


def frontier(d):
    fig, ax = plt.subplots(figsize=(3.4, 2.3))
    base = d[(d["qc_history"] == 120) & (d["context_refresh"] == 60)]
    for label, name, color, marker in RUNS:
        g = base[base["season"] == label].sort_values("decision_latency_min")
        ax.plot(g["decision_latency_min"], g["rmse_ugm3"], marker=marker, color=color, ls="none", ms=4.5,
                alpha=0.85, label=name)
    for x, text in ((31, "10-min windows,\nwatermark 0/5/15 min"), (51, "30-min\nwindows"), (81, "60-min\nwindows")):
        ax.annotate(text, (x, 0.68), ha="center", va="bottom", fontsize=6.5, color="#555555")
    ax.set_ylim(0, 0.8)
    ax.set_xlabel("Decision latency after window start (min)")
    ax.set_ylabel("RMSE vs. hindsight (µg/m³)")
    ax.legend(frameon=False, ncol=2, loc="center left", bbox_to_anchor=(0.0, 0.45))
    ax.grid(alpha=0.25, lw=0.5)
    save(fig, "frontier")


def context(d):
    fig, ax = plt.subplots(figsize=(3.4, 2.1))
    g = d[(d["window"] == 10) & (d["watermark"] == 5) & (d["qc_history"] == 120)]
    for label, name, color, marker in RUNS:
        s = g[g["season"] == label].sort_values("context_refresh")
        ax.plot(s["context_refresh"], 100 * s["label_match"], marker=marker, color=color, lw=1, ms=4, label=name)
    ax.set_xscale("log")
    ax.set_xticks([15, 60, 360], ["15", "60", "360"])
    ax.xaxis.set_minor_formatter(matplotlib.ticker.NullFormatter())
    ax.set_xlabel("Context refresh interval (min, log scale)")
    ax.set_ylabel("Label agreement with hindsight (%)")
    ax.legend(frameon=False, ncol=2, loc="lower left")
    ax.grid(alpha=0.25, lw=0.5)
    save(fig, "context_refresh")


def privacy():
    fig, ax = plt.subplots(figsize=(3.4, 2.2))
    d = pd.read_csv(OUT_DATA / "privacy_tradeoff_clip100.csv")
    d = d[d["epsilon"] != "none"].assign(epsilon=lambda x: x["epsilon"].astype(float))
    for k, color, marker in ((1, "#999999", "o"), (3, "#0072B2", "s"), (5, "#D55E00", "^")):
        s = d[d["k"] == k].sort_values("epsilon")
        ax.plot(s["epsilon"], 100 * s["alert_recall"], marker=marker, color=color, lw=1, ms=4,
                label=f"k = {k} ({100 * s['published_share'].iloc[0]:.0f}% of hexagon-hours published)")
    ax.set_xscale("log")
    ax.set_xticks([0.1, 0.5, 1, 2, 10], ["0.1", "0.5", "1", "2", "10"])
    ax.xaxis.set_minor_formatter(matplotlib.ticker.NullFormatter())
    ax.set_xlabel("Privacy budget ε per hourly release (log scale)")
    ax.set_ylabel("AQI>100 alerts kept (%)")
    ax.set_ylim(40, 100)
    ax.legend(frameon=False, loc="lower right")
    ax.grid(alpha=0.25, lw=0.5)
    save(fig, "privacy")


def latency_split():
    import json
    s = json.loads((OUT_DATA / "demo_live" / "latency_split.json").read_text())
    fig, ax = plt.subplots(figsize=(3.4, 1.35))
    rows = [("Median", s["designed_wait_wall_s_p50"], s["overhead_s_p50"]),
            ("95th pct.", s["latency_s_p95"] - s["overhead_s_p95"], s["overhead_s_p95"])]
    for i, (name, wait, over) in enumerate(rows):
        ax.barh(i, wait, color="#E69F00", label="Designed wait (watermark)" if i == 0 else None)
        ax.barh(i, over, left=wait, color="#D55E00", label="System overhead" if i == 0 else None)
        ax.text(wait / 2, i, f"{wait:.1f}", ha="center", va="center", color="white", fontsize=7)
        ax.text(wait + over / 2, i, f"{over:.1f}", ha="center", va="center", color="white", fontsize=7)
    ax.set_yticks([0, 1], [r[0] for r in rows])
    ax.invert_yaxis()
    ax.set_xlabel("End-to-end latency, Kafka to MongoDB Atlas (s)")
    ax.legend(frameon=False, ncol=2, loc="lower center", bbox_to_anchor=(0.5, 1.0))
    save(fig, "latency_split")


def coherence():
    d = pd.read_csv(OUT_DATA / "lsh_sensors.days.csv")
    order = [("clean", "Clean"), ("moderate", "Moderate"), ("smoke", "Smoke"), ("july4", "July 4")]
    fig, ax = plt.subplots(figsize=(3.4, 1.9))
    for i, (cls, name) in enumerate(order):
        v = d[d["class"] == cls]["coherence"]
        jitter = [(j % 5 - 2) * 0.05 for j in range(len(v))]
        ax.scatter([i + j for j in jitter], v, s=12, color="#0072B2" if cls != "smoke" else "#D55E00", alpha=0.75)
        ax.plot([i - 0.25, i + 0.25], [v.mean()] * 2, color="#333333", lw=1.2)
        ax.text(i + 0.3, v.mean(), f"{v.mean():.2f}", va="center", fontsize=6.5, color="#333333")
    ax.set_xticks(range(len(order)), [f"{n}\n({(d['class'] == c).sum()} days)" for c, n in order])
    ax.set_ylabel("Coherence")
    ax.grid(axis="y", alpha=0.25, lw=0.5)
    save(fig, "coherence")


def dgim_trace():
    from collections import deque
    from analysis.stream_summaries import corrected, from_replay
    from common.stream_algos import DGIM
    n, dg, window, xs, est, exact = 10_000, DGIM(10_000), deque(maxlen=10_000), [], [], []
    for i, m in enumerate(from_replay(OUT_DATA / "season_2020" / "replay_measured")):
        pm = corrected(m)
        if pm is None:
            continue
        bit = pm > 35.4
        dg.add(bit)
        window.append(bit)
        if len(window) == n and i % 200 == 0:
            xs.append(pd.to_datetime(m["arrival_time"], unit="s"))
            est.append(dg.count())
            exact.append(sum(window))
    fig, ax = plt.subplots(figsize=(3.4, 1.8))
    ax.plot(xs, exact, color="#333333", lw=1.2, label="Exact (10,000 bits)")
    ax.plot(xs, est, color="#D55E00", lw=0.9, label=f"DGIM ({dg.nbytes} bytes at the end)")
    ax.set_ylabel("Elevated readings\namong last 10,000")
    ax.legend(frameon=False, loc="upper left")
    ax.grid(alpha=0.25, lw=0.5)
    fig.autofmt_xdate()
    save(fig, "dgim")


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    d = sweeps()
    frontier(d)
    context(d)
    privacy()
    latency_split()
    coherence()
    dgim_trace()
    shutil.copy(OUT_DATA / "overview" / "season_2020_compact.png", OUT / "season_2020.png")
    print(f"Figures in {OUT}")


if __name__ == "__main__":
    main()
