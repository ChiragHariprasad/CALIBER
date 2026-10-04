"""
Generate the three EDA charts used in the CALIBRE PPT.

Run:
    python 03_make_eda_charts.py

Matplotlib only. No hand-entered chart values: every plotted value is read
from the processed CSVs produced by 02_eda_metrics.py.
"""

import pandas as pd
import matplotlib.pyplot as plt

from EDA.figures.config import PROCESSED_DIR, FIG_DIR


def pit_chart():
    df = pd.read_csv(PROCESSED_DIR / "pit_lane_summary.csv")

    order = [
        "green_flag_stop",
        "drive_through",
        "sc_stop_laps_58_61",
        "sc_stop_laps_65_66",
        "restart_via_pit_lane",
    ]
    labels = [
        "Green-flag stop",
        "Drive-through",
        "SC stop, laps 58–61",
        "SC stop, laps 65–66",
        "Restart via pit lane",
    ]

    df = df.set_index("passage_type").reindex(order).reset_index()

    fig, ax = plt.subplots(figsize=(10, 5.5))
    y = range(len(df))

    for i, row in df.iterrows():
        ax.plot(
            [row["min"], row["max"]],
            [i, i],
            linewidth=2
        )
        ax.plot(
            row["median"],
            i,
            marker="o",
            markersize=8
        )
        ax.text(
            row["median"] + 0.8,
            i,
            f"{row['median']:.1f}",
            va="center"
        )

    ax.set_yticks(list(y))
    ax.set_yticklabels(labels)
    ax.set_xlabel("Time in pit lane (s)")
    ax.set_title("Pit-lane time by passage type")
    ax.grid(axis="x", alpha=0.25)
    fig.tight_layout()
    fig.savefig(FIG_DIR / "eda1_pit_lane_time.png", dpi=220)
    plt.close(fig)


def track_limit_chart():
    df = pd.read_csv(PROCESSED_DIR / "track_limit_by_session.csv")

    # Map session keys to the labels used in the CALIBRE report.
    session_labels = {
        11292: "FP1",
        11293: "FP2",
        11294: "FP3",
        11295: "Qualifying",
        11299: "Race",
    }

    turns = ["Turn 10", "Turn 16", "Turn 1", "Other"]
    wide = (
        df.pivot_table(
            index="session_key",
            columns="turn",
            values="deletions",
            aggfunc="sum",
            fill_value=0,
        )
        .reindex(columns=[t for t in turns if t in df["turn"].unique()], fill_value=0)
        .fillna(0)
        .sort_index()
    )

    # Aggregate every extracted corner not in the report's three named
    # corners into "Other".
    named_cols = [c for c in ["Turn 10", "Turn 16", "Turn 1"] if c in wide.columns]
    other_cols = [c for c in wide.columns if c not in named_cols]
    if other_cols:
        wide["Other"] = wide[other_cols].sum(axis=1)
        wide = wide.drop(columns=other_cols)

    for c in turns:
        if c not in wide.columns:
            wide[c] = 0
    wide = wide[turns]

    fig, ax = plt.subplots(figsize=(8.5, 5.2))
    x = range(len(wide))
    bottom = [0] * len(wide)

    for turn in turns:
        values = wide[turn].tolist()
        ax.bar(x, values, bottom=bottom, label=turn)
        bottom = [b + v for b, v in zip(bottom, values)]

    ax.set_xticks(list(x))
    ax.set_xticklabels([session_labels.get(int(sk), str(sk)) for sk in wide.index])
    ax.set_ylabel("Deletions (count)")
    ax.set_title("Track-limit deletions by session")
    ax.legend(title="Corner")
    ax.grid(axis="y", alpha=0.25)
    fig.tight_layout()
    fig.savefig(FIG_DIR / "eda2_track_limit_concentration.png", dpi=220)
    plt.close(fig)


def penalty_latency_chart():
    df = pd.read_csv(PROCESSED_DIR / "penalty_latency_summary.csv")
    df = df[df["valid_match"].astype(bool)].copy()

    # Sort by delay to mirror a clean horizontal ranking.
    df = df.sort_values("delay_min").reset_index(drop=True)

    labels = [
        f"{row['driver']} (lap {int(row['pit_lap'])} stop)"
        for _, row in df.iterrows()
    ]

    fig, ax = plt.subplots(figsize=(8.5, 5.2))
    y = range(len(df))
    ax.barh(list(y), df["delay_min"])
    ax.set_yticks(list(y))
    ax.set_yticklabels(labels)
    ax.set_xlabel("Minutes from pit-lane passage to race-control notice")
    ax.set_title("Pit speeding: event-to-notice latency")

    for i, value in enumerate(df["delay_min"]):
        ax.text(value + 0.05, i, f"{value:.1f}", va="center")

    ax.grid(axis="x", alpha=0.25)
    fig.tight_layout()
    fig.savefig(FIG_DIR / "eda3_penalty_latency.png", dpi=220)
    plt.close(fig)


def main():
    pit_chart()
    track_limit_chart()
    penalty_latency_chart()
    print("Charts written to:", FIG_DIR)


if __name__ == "__main__":
    main()
