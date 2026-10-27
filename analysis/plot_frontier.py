#!/usr/bin/env python3
"""Paper figures from knob-sweep results (the CSV written by analysis/run_sweep.py).

Writes to --out:
  frontier_<metric>.png / .pdf  Decision latency vs. error for every setting, one
                                color and marker per season. Settings that no other
                                setting beats on both axes (the Pareto frontier) are
                                joined by a line; the rest are faded. This is the
                                headline figure for RQ2.
  knob_effects.png / .pdf       Small multiples: each knob varied on its own (others
                                at the default), one row per metric.
  frontier_points.csv / .md     Every frontier point as a table, so no value is
                                readable only from the chart.

Usage:
  python analysis/plot_frontier.py --results data/out/sweep.csv --out figures
  python analysis/plot_frontier.py --results data/out/sweep.csv --out figures --metric label_disagreement
"""

import argparse
import csv
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import matplotlib  # noqa: E402

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from analysis.run_sweep import DEFAULT  # noqa: E402

KNOBS = ["window", "watermark", "qc_history", "context_refresh"]
KNOB_TITLES = {"window": "Window", "watermark": "Late-data tolerance",
               "qc_history": "QC history", "context_refresh": "Context refresh"}
KNOB_SHORT = {"window": "w", "watermark": "wm", "qc_history": "q", "context_refresh": "c"}

# name -> (axis label, how to compute it from a results row). Frontier metrics are
# all "lower is better", so the frontier is the lower-left edge of the scatter.
FRONTIER_METRICS = {
    "rmse_ugm3": ("PM2.5 error vs. hindsight (RMSE, µg/m³)", lambda r: r["rmse_ugm3"]),
    "mae_ugm3": ("PM2.5 error vs. hindsight (MAE, µg/m³)", lambda r: r["mae_ugm3"]),
    "missed_readings": ("Readings missed (share)", lambda r: r["missed_readings"]),
    "label_disagreement": ("Smoke labels differing from hindsight (share)", lambda r: 1 - r["label_match"]),
    "smoke_alert_miss": ("Smoke alerts missed (1 − recall)", lambda r: 1 - r["smoke_alert_recall"]),
    # Against regulatory monitors (sweep run with --monitors):
    "mon_rmse_ugm3": ("PM2.5 error vs. regulatory monitors (RMSE, µg/m³)", lambda r: r["mon_rmse_ugm3"]),
    "mon_smoke_alert_miss": ("Unhealthy hours missed vs. monitors (1 − recall)",
                             lambda r: 1 - r["mon_smoke_alert_recall"]),
    "mon_smoke_false_alarm_rate": ("False alarms vs. monitors (share of healthy hours)",
                                   lambda r: r["mon_smoke_false_alarm_rate"]),
}
EFFECT_ROWS = {
    "hindsight": [
        ("decision_latency_min", "Decision latency\n(min)"),
        ("rmse_ugm3", "PM2.5 RMSE\n(µg/m³)"),
        ("label_match", "Label agreement\n(share)"),
        ("smoke_alert_recall", "Smoke-alert recall\n(share)"),
    ],
    "monitors": [
        ("decision_latency_min", "Decision latency\n(min)"),
        ("mon_rmse_ugm3", "RMSE vs. monitors\n(µg/m³)"),
        ("mon_smoke_alert_recall", "Unhealthy hours\ncaught (share)"),
        ("mon_lead_median_min", "Lead over official\nvalue (min)"),
    ],
}

# Reference palette, light mode, first three categorical slots: validated all-pairs
# on white (CVD ΔE ≥ 9.2, normal-vision ΔE ≥ 24). Aqua is under 3:1 contrast on
# white, so every season also gets its own marker and the values go to a table.
SERIES = ["#2a78d6", "#eb6834", "#1baf7a"]
MARKERS = ["o", "s", "^"]
SURFACE, INK, INK_2, MUTED, GRID, AXIS = "#ffffff", "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7"


def style():
    logging.getLogger("matplotlib.font_manager").setLevel(logging.ERROR)
    logging.getLogger("fontTools").setLevel(logging.ERROR)  # harmless notes about macOS Helvetica
    plt.rcParams.update({
        "font.family": "sans-serif",
        "font.sans-serif": ["Helvetica", "Arial", "DejaVu Sans"],
        "font.size": 9,
        "axes.facecolor": SURFACE, "figure.facecolor": SURFACE, "savefig.facecolor": SURFACE,
        "axes.edgecolor": AXIS, "axes.linewidth": 0.8, "axes.labelcolor": INK_2,
        "axes.titlesize": 9.5, "axes.titleweight": "bold", "axes.titlecolor": INK,
        "axes.spines.top": False, "axes.spines.right": False,
        "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6, "grid.linestyle": "-",
        "axes.axisbelow": True,
        "xtick.color": MUTED, "ytick.color": MUTED, "xtick.labelcolor": INK_2, "ytick.labelcolor": INK_2,
        "xtick.major.size": 0, "ytick.major.size": 0,
        "legend.frameon": False, "legend.labelcolor": INK_2,
        "pdf.fonttype": 42,  # embed TrueType so journals accept the PDF
    })


def load(path):
    numeric = {"window", "watermark", "qc_history", "context_refresh", "decision_latency_min", "coverage",
               "rmse_ugm3", "mae_ugm3", "category_match", "missed_readings", "alert_precision", "alert_recall",
               "smoke_alert_precision", "smoke_alert_recall", "label_match", "baseline_windows"}
    rows = []
    with open(path) as f:
        for r in csv.DictReader(f):
            row = dict(r)
            for k in (numeric | {c for c in r if c.startswith(("mon_", "base_mon_"))}) & set(r):
                row[k] = float(r[k]) if r[k] not in ("", "None") else None
            for k in KNOBS:
                row[k] = int(row[k])
            rows.append(row)
    if not rows:
        sys.exit(f"{path} has no results.")
    return rows


def pareto(points):
    """Points (x, y, row) not beaten on both axes by another point; sorted by x."""
    front = []
    for p in sorted(points, key=lambda p: (p[0], p[1])):
        if not front or p[1] < front[-1][1]:
            front.append(p)
    return front


def setting_tag(row):
    """Compact name for a setting: the knobs that differ from the default."""
    diff = [f"{KNOB_SHORT[k]}{row[k]}" for k in KNOBS if row[k] != DEFAULT[k]]
    return " ".join(diff) if diff else "default"


def place_labels(ax, items):
    """Direct-label points without collisions: try a few offsets around each point,
    keep the first that overlaps no earlier label or marker, otherwise skip it
    (every value is also in frontier_points.csv/.md)."""
    fig = ax.figure
    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()
    taken = []
    for x, y, _ in items:  # markers count as taken space too
        px, py = ax.transData.transform((x, y))
        taken.append((px - 6, py - 6, px + 6, py + 6))
    offsets = [(6, 5), (6, -12), (-6, 5), (-6, -12)]
    for x, y, text in items:
        for dx, dy in offsets:
            # A thin surface-colored backing keeps the label readable where it crosses a line.
            ann = ax.annotate(text, (x, y), xytext=(dx, dy), textcoords="offset points", fontsize=7.5,
                              color=INK_2, ha="left" if dx > 0 else "right", zorder=5,
                              bbox={"boxstyle": "square,pad=0.1", "facecolor": SURFACE, "edgecolor": "none"})
            box = ann.get_window_extent(renderer)
            rect = (box.x0 - 2, box.y0 - 1, box.x1 + 2, box.y1 + 1)
            inside = ax.get_window_extent(renderer)
            fits = inside.x0 <= rect[0] and rect[2] <= inside.x1 and inside.y0 <= rect[1] and rect[3] <= inside.y1
            if fits and not any(r[0] < t[2] and t[0] < r[2] and r[1] < t[3] and t[1] < r[3]
                                for r in [rect] for t in taken):
                taken.append(rect)
                break
            ann.remove()


def save(fig, out, name, formats):
    for fmt in formats:
        fig.savefig(out / f"{name}.{fmt}", dpi=300, bbox_inches="tight")
    plt.close(fig)


def plot_frontier(rows, seasons, metric, out, formats, label_season):
    label, value = FRONTIER_METRICS[metric]
    fig, ax = plt.subplots(figsize=(6.4, 4.2))
    table, to_label = [], []
    for i, season in enumerate(seasons):
        color, marker = SERIES[i], MARKERS[i]
        pts = [(r["decision_latency_min"], value(r), r) for r in rows
               if r["season"] == season and r["decision_latency_min"] is not None and value_ok(value, r)]
        front = pareto(pts)
        on_front = {id(p[2]) for p in front}
        rest = [p for p in pts if id(p[2]) not in on_front]
        # Dominated settings: same identity, faded, so the frontier carries the eye.
        ax.scatter([p[0] for p in rest], [p[1] for p in rest], s=30, marker=marker, color=color,
                   alpha=0.3, linewidths=0, zorder=2)
        ax.plot([p[0] for p in front], [p[1] for p in front], color=color, linewidth=1.5,
                solid_joinstyle="round", solid_capstyle="round", zorder=3)
        ax.scatter([p[0] for p in front], [p[1] for p in front], s=46, marker=marker, color=color,
                   edgecolors=SURFACE, linewidths=1.5, zorder=4, label=season)
        if season == label_season:
            to_label = [(x, y, setting_tag(r)) for x, y, r in front]
        table += [{"season": season, "setting": setting_tag(r), "decision_latency_min": round(x, 2),
                   metric: round(y, 4), "config_id": r["stream_config"]} for x, y, r in front]

    ax.set_xlabel("Decision latency (minutes from window start to result)")
    ax.set_ylabel(label)
    ax.set_xlim(left=0)
    ax.set_ylim(bottom=0)
    place_labels(ax, to_label)
    title = "Latency-accuracy frontier"
    if len(seasons) == 1:
        title += f", {seasons[0]} season"   # one series: the title names it, no legend box
    else:
        # Above the plot, so it never covers points.
        ax.legend(loc="lower right", bbox_to_anchor=(1.0, 1.0), ncol=len(seasons), fontsize=8,
                  handletextpad=0.3, columnspacing=1.2, borderaxespad=0.2)
    ax.set_title(title, loc="left", pad=14)
    ax.text(0, 1.0, "Lower-left is better. Faded points are settings beaten on both axes.",
            transform=ax.transAxes, fontsize=7.5, color=MUTED, va="bottom", ha="left")
    save(fig, out, f"frontier_{metric}", formats)
    return table


def value_ok(value, row):
    try:
        return value(row) is not None
    except (TypeError, KeyError):
        return False


def plot_knob_effects(rows, seasons, out, formats, which):
    effect_rows = EFFECT_ROWS[which]
    fig, axes = plt.subplots(len(effect_rows), len(KNOBS), figsize=(9.2, 7.6), sharey="row")
    for col, knob in enumerate(KNOBS):
        others = [k for k in KNOBS if k != knob]
        levels = sorted({r[knob] for r in rows if all(r[k] == DEFAULT[k] for k in others)})
        pos = {lvl: i for i, lvl in enumerate(levels)}  # evenly spaced levels, labeled with values
        for row_i, (metric, metric_label) in enumerate(effect_rows):
            ax = axes[row_i][col]
            for s_i, season in enumerate(seasons):
                pts = sorted((r[knob], r[metric]) for r in rows if r["season"] == season
                             and all(r[k] == DEFAULT[k] for k in others) and r.get(metric) is not None)
                if not pts:
                    continue
                ax.plot([pos[x] for x, _ in pts], [y for _, y in pts], color=SERIES[s_i], linewidth=1.5,
                        marker=MARKERS[s_i], markersize=5.5, markeredgecolor=SURFACE, markeredgewidth=1.2,
                        solid_joinstyle="round", label=season)
            ax.set_xticks(range(len(levels)))
            ax.set_xticklabels([str(lvl) for lvl in levels])
            ax.set_xlim(-0.3, len(levels) - 0.7)
            # Mark the default level so each panel shows where the other panels sit.
            if DEFAULT[knob] in pos:
                ax.axvline(pos[DEFAULT[knob]], color=AXIS, linewidth=0.8, zorder=1)
            if row_i == 0:
                ax.set_title(f"{KNOB_TITLES[knob]} (min)", loc="left")
            if col == 0:
                ax.set_ylabel(metric_label)
    handles, labels = axes[0][0].get_legend_handles_labels()
    if len(seasons) > 1 and handles:
        fig.legend(handles, labels, loc="upper right", ncol=len(seasons), fontsize=8, bbox_to_anchor=(0.995, 1.0))
    subtitle = "Each knob varied alone; the others stay at the default (gray line)."
    if len(seasons) == 1:
        subtitle = f"{seasons[0]} season. " + subtitle
    # Plain text rather than suptitle, so tight_layout does not reserve extra space for it.
    fig.text(0.01, 0.995, "What each knob does", ha="left", va="top", fontsize=10.5, fontweight="bold", color=INK)
    fig.text(0.01, 0.968, subtitle, ha="left", va="top", fontsize=8, color=MUTED)
    fig.tight_layout(rect=(0, 0, 1, 0.945), h_pad=1.2, w_pad=1.0)
    save(fig, out, "knob_effects" if which == "hindsight" else "knob_effects_monitors", formats)


def write_table(table, metric, out):
    if not table:
        return
    with (out / "frontier_points.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(table[0]))
        w.writeheader()
        w.writerows(table)
    lines = [f"| Season | Setting | Decision latency (min) | {metric} |", "|---|---|---|---|"]
    lines += [f"| {r['season']} | {r['setting']} | {r['decision_latency_min']} | {r[metric]} |" for r in table]
    (out / "frontier_points.md").write_text("\n".join(lines) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--results", default=str(ROOT / "data" / "out" / "sweep.csv"))
    parser.add_argument("--out", default=str(ROOT / "figures"))
    parser.add_argument("--metric", choices=sorted(FRONTIER_METRICS), default="rmse_ugm3",
                        help="y-axis of the frontier figure (all lower-is-better)")
    parser.add_argument("--seasons", help="comma-separated season labels to include, in legend order")
    parser.add_argument("--label-season", help="season whose frontier points get setting labels "
                                               "(default: the first season)")
    parser.add_argument("--rows", choices=sorted(EFFECT_ROWS), default="hindsight",
                        help="knob-effects metrics: vs. the hindsight baseline, or vs. regulatory monitors")
    parser.add_argument("--formats", default="png,pdf")
    args = parser.parse_args()

    rows = load(args.results)
    seasons = args.seasons.split(",") if args.seasons else sorted({r["season"] for r in rows})
    if len(seasons) > len(SERIES):
        sys.exit(f"{len(seasons)} seasons, but scatter plots stay colorblind-safe only up to {len(SERIES)}. "
                 "Pick up to three with --seasons, or plot them separately.")
    rows = [r for r in rows if r["season"] in seasons]
    needed = [args.metric] + ([m for m, _ in EFFECT_ROWS["monitors"]] if args.rows == "monitors" else [])
    missing = [m for m in needed if m.startswith("mon_") and not any(m in r or m.replace("_miss", "_recall") in r
                                                                       for r in rows)]
    if missing:
        sys.exit(f"{', '.join(missing)} not in the results; run the sweep with --monitors.")
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    formats = args.formats.split(",")

    style()
    table = plot_frontier(rows, seasons, args.metric, out, formats, args.label_season or seasons[0])
    plot_knob_effects(rows, seasons, out, formats, args.rows)
    write_table(table, args.metric, out)
    print(f"Wrote figures and frontier table to {out}/")


if __name__ == "__main__":
    main()
