"""
Reproduce the three EDA findings used in the CALIBRE PPT.

Outputs:
  - pit_lane_summary.csv
  - track_limit_summary.csv
  - penalty_latency_summary.csv
  - console verification numbers

Run:
    python 02_eda_metrics.py
"""

import json
import re
from pathlib import Path

import numpy as np
import pandas as pd

from EDA.figures.config import (
    RAW_DIR,
    PROCESSED_DIR,
    RACE_SESSION_KEY,
    RED_FLAG_HOLD_THRESHOLD_S,
    RESTART_THRESHOLD_S,
    SC1_LAPS,
    SC2_LAPS,
    DRIVE_THROUGH_LAPS,
)


def load_json(name):
    path = RAW_DIR / f"{name}.json"
    with path.open("r", encoding="utf-8") as f:
        return pd.DataFrame(json.load(f))


def load_session_endpoint(endpoint, session_key):
    return load_json(f"{endpoint}_session_{session_key}")


def prepare_pit():
    pit = load_session_endpoint("pit", RACE_SESSION_KEY)
    pit["date"] = pd.to_datetime(pit["date"], format="ISO8601", utc=True)
    pit = pit.sort_values(["date", "driver_number"]).reset_index(drop=True)

    # Cleaning step from the report.
    red_flag = pit["lane_duration"].astype(float) > RED_FLAG_HOLD_THRESHOLD_S

    pit["passage_type"] = "green_flag_stop"

    # Restart via pit lane forms a distinct, long but non-red-flag cluster.
    pit.loc[
        pit["lane_duration"].astype(float) >= RESTART_THRESHOLD_S,
        "passage_type",
    ] = "restart_via_pit_lane"

    pit.loc[red_flag, "passage_type"] = "red_flag_hold"

    pit.loc[
        pit["lap_number"].isin(DRIVE_THROUGH_LAPS) & ~red_flag,
        "passage_type",
    ] = "drive_through"

    pit.loc[
        pit["lap_number"].isin(SC1_LAPS) & ~red_flag,
        "passage_type",
    ] = "sc_stop_laps_58_61"

    pit.loc[
        pit["lap_number"].isin(SC2_LAPS) & ~red_flag,
        "passage_type",
    ] = "sc_stop_laps_65_66"

    return pit


def track_limit_events():
    frames = []
    sessions = sorted(
        int(p.stem.split("_")[-1])
        for p in RAW_DIR.glob("race_control_session_*.json")
    )

    for sk in sessions:
        rc = load_session_endpoint("race_control", sk)
        if not rc.empty:
            rc["session_key"] = sk
            rc = rc.dropna(how="all", axis=1)
            frames.append(rc)

    rc = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    rc["date"] = pd.to_datetime(rc["date"], format="ISO8601", utc=True)
    rc["message"] = rc["message"].fillna("").astype(str)

    # Candidate deletion messages.
    mask = (
        rc["message"].str.contains("deleted", case=False, regex=False)
        & rc["message"].str.contains("track limit", case=False, regex=False)
    )
    deletions = rc.loc[mask].copy()

    # OpenF1 does not populate driver_number and lap_number on race_control text notices.
    # Extract them from the message text when present (e.g. "CAR 16 ... LAP 2").
    deletions["driver_number"] = deletions.get("driver_number", pd.Series(dtype=object)).fillna(
        deletions["message"].str.extract(r"CAR\s+([0-9]+)", flags=re.I)[0]
    )
    deletions["lap_number"] = deletions.get("lap_number", pd.Series(dtype=object)).fillna(
        deletions["message"].str.extract(r"LAP\s+([0-9]+)", flags=re.I)[0]
    )

    # Deduplicate only identical records for the same session and message.
    deletions = deletions.drop_duplicates(subset=["session_key", "message"], keep="first")

    # Extract "Turn 10", "Turn 16", etc. from the message when present.
    deletions["turn"] = (
        deletions["message"]
        .str.extract(r"(?:turn|corner)\s*([0-9]+)", flags=re.I, expand=False)
    )
    deletions["turn"] = deletions["turn"].apply(
        lambda x: f"Turn {int(x)}" if pd.notna(x) else "Other"
    )

    summary = (
        deletions.groupby(["session_key", "turn"])
        .size()
        .reset_index(name="deletions")
    )

    # Also keep a simple all-weekend summary for verification.
    weekend_summary = (
        deletions["turn"]
        .value_counts()
        .rename_axis("turn")
        .reset_index(name="deletions")
    )

    return deletions, summary, weekend_summary


def driver_map():
    path = RAW_DIR / f"drivers_session_{RACE_SESSION_KEY}.json"
    if not path.exists():
        return {}
    df = load_json(f"drivers_session_{RACE_SESSION_KEY}")
    mapping = {}
    for _, row in df.iterrows():
        if pd.notna(row.get("driver_number")):
            num = int(row["driver_number"])
            mapping[num] = row.get("name_acronym") or str(num)
    return mapping


def penalty_latency():
    rc = load_session_endpoint("race_control", RACE_SESSION_KEY)
    pit = load_session_endpoint("pit", RACE_SESSION_KEY)

    rc["date"] = pd.to_datetime(rc["date"], format="ISO8601", utc=True)
    pit["date"] = pd.to_datetime(pit["date"], format="ISO8601", utc=True)

    rc["message"] = rc["message"].fillna("").astype(str)

    # Race-control messages describing pit-lane speeding penalties.
    # Exclude "served" notices to focus on the 6 penalty decisions.
    mask = (
        rc["message"].str.contains("pit lane", case=False, regex=False)
        & rc["message"].str.contains("speed", case=False, regex=False)
        & rc["message"].str.contains("penalty", case=False, regex=False)
        & ~rc["message"].str.contains("served", case=False, regex=False)
    )
    penalties = rc.loc[mask].copy().sort_values("date")
    pit = pit.sort_values("date").copy()

    # OpenF1 does not populate driver_number on race_control text notices.
    # Extract driver number from "CAR <number>" in message text.
    extracted_driver = penalties["message"].str.extract(r"CAR\s+([0-9]+)", flags=re.I)[0]
    penalties["driver_number"] = penalties["driver_number"].fillna(extracted_driver)
    penalties["driver_number"] = pd.to_numeric(penalties["driver_number"], errors="coerce")

    rows = []
    used_pit_indices = set()

    for idx, p in penalties.iterrows():
        driver = p.get("driver_number")
        candidates = pit[
            (pit["date"] <= p["date"])
            & (pit["date"] >= p["date"] - pd.Timedelta(minutes=15))
        ].copy()

        if pd.notna(driver):
            candidates = candidates[candidates["driver_number"] == driver]

        if candidates.empty:
            rows.append({
                "driver_number": driver,
                "penalty_time": p["date"],
                "penalty_message": p["message"],
                "pit_lap": np.nan,
                "pit_time": pd.NaT,
                "delay_min": np.nan,
            })
            continue

        # Prefer the latest prior unused pit passage.
        candidates = candidates.sort_values("date", ascending=False)
        chosen = None
        for pit_idx, candidate in candidates.iterrows():
            if pit_idx not in used_pit_indices:
                chosen = candidate
                used_pit_indices.add(pit_idx)
                break

        if chosen is None:
            chosen = candidates.iloc[0]

        delay_min = (p["date"] - chosen["date"]).total_seconds() / 60.0
        rows.append({
            "driver_number": driver,
            "penalty_time": p["date"],
            "penalty_message": p["message"],
            "pit_lap": chosen.get("lap_number"),
            "pit_time": chosen["date"],
            "delay_min": delay_min,
        })

    out = pd.DataFrame(rows)

    # The report notes that one of six race penalties cannot be tied
    # to a single passage from public data. Keep unmatched/implausible
    # associations visible instead of inventing a match.
    out["valid_match"] = (
        out["delay_min"].notna()
        & out["delay_min"].between(0, 15)
    )

    names = driver_map()
    out["driver_number_int"] = pd.to_numeric(out["driver_number"], errors="coerce").astype("Int64")
    out["driver"] = out["driver_number_int"].map(names).fillna(out["driver_number_int"].astype(str))

    return penalties, out.sort_values("penalty_time")


def main():
    # ---------- EDA 1 ----------
    pit = prepare_pit()
    plotted = pit[pit["passage_type"] != "red_flag_hold"].copy()

    pit_summary = (
        plotted.groupby("passage_type")["lane_duration"]
        .agg(["count", "median", "min", "max"])
        .reset_index()
    )
    pit_summary.to_csv(PROCESSED_DIR / "pit_lane_summary.csv", index=False)

    print("\n=== EDA 1: PIT-LANE ===")
    print("Total race pit records:", len(pit))
    print("Red-flag holds:", (pit["passage_type"] == "red_flag_hold").sum())
    print("Plotted passages:", len(plotted))
    print(pit_summary.to_string(index=False))

    # ---------- EDA 2 ----------
    deletions, turn_by_session, weekend_summary = track_limit_events()
    turn_by_session.to_csv(PROCESSED_DIR / "track_limit_by_session.csv", index=False)
    weekend_summary.to_csv(PROCESSED_DIR / "track_limit_summary.csv", index=False)
    deletions.to_csv(PROCESSED_DIR / "track_limit_events.csv", index=False)

    print("\n=== EDA 2: TRACK LIMITS ===")
    print("Unique track-limit deletions (all-weekend):", len(deletions))
    print(weekend_summary.to_string(index=False))

    # ---------- EDA 3 ----------
    penalty_events, latency = penalty_latency()
    latency.to_csv(PROCESSED_DIR / "penalty_latency_summary.csv", index=False)

    matched = latency[latency["valid_match"]].copy()

    print("\n=== EDA 3: PENALTY LATENCY ===")
    print("Pit-speeding penalty messages detected:", len(penalty_events))
    print("Matched to a prior pit passage:", len(matched))
    if not matched.empty:
        print(
            matched[
                ["driver", "pit_lap", "delay_min"]
            ].to_string(index=False)
        )
        print(
            f"Matched delay range: {matched['delay_min'].min():.1f}"
            f"-{matched['delay_min'].max():.1f} min"
        )

    # ---------- Report verification ----------
    print("\n=== REPORT CHECKS ===")
    pre_race_deletions = deletions[deletions["session_key"] != RACE_SESSION_KEY]
    pre_race_turn10 = pre_race_deletions[pre_race_deletions["turn"] == "Turn 10"]

    expected = {
        "race_pit_records": 86,
        "red_flag_holds": 16,
        "plotted_passages": 70,
        "ordinary_passages": 61,
        "pre_race_tl_deletions": 198,
        "all_weekend_tl_deletions": 228,
        "pre_race_turn_10": 127,
    }
    actual = {
        "race_pit_records": len(pit),
        "red_flag_holds": int((pit["passage_type"] == "red_flag_hold").sum()),
        "plotted_passages": len(plotted),
        "ordinary_passages": int(
            pit["passage_type"].isin(
                ["green_flag_stop", "drive_through"]
            ).sum()
            + int((pit["passage_type"] == "sc_stop_laps_58_61").sum())
            + int((pit["passage_type"] == "sc_stop_laps_65_66").sum())
        ),
        "pre_race_tl_deletions": len(pre_race_deletions),
        "all_weekend_tl_deletions": len(deletions),
        "pre_race_turn_10": len(pre_race_turn10),
    }

    for key, exp in expected.items():
        got = actual.get(key)
        status = "OK" if got == exp else "CHECK"
        print(f"{key:24s} expected={exp!s:>6s} actual={got!s:>6s} [{status}]")

    print("\nOutputs written to:", PROCESSED_DIR)


if __name__ == "__main__":
    main()
