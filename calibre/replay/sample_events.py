"""
CALIBRE – Sample Event Data
=============================
Deterministic sample dataset for immediate demo without OpenF1 download.

Contains pit-lane telemetry events from multiple drivers covering:
  - CLEAR readings (well below limit)
  - DEFER readings (near-threshold, within uncertainty)
  - BREACH readings (clearly above limit)
  - Normal passages
  - Corresponding simulated timing-loop observations

All timing data is explicitly marked ``source="simulation"``
because real FIA timing-loop data is not publicly available.
"""

from __future__ import annotations

SAMPLE_TELEMETRY: list[dict] = [
    # === Driver 1 (VER) – normal pit passage, well clear ===
    {
        "timestamp": "2024-05-26T14:30:00Z",
        "session_key": 9999,
        "driver_number": 1,
        "speed": 48.2,
        "throttle": 30.0,
        "brake": 0.0,
        "gear": 2,
        "rpm": 5200.0,
        "source": "simulation",
        "zone": "pit_entry",
    },
    {
        "timestamp": "2024-05-26T14:30:05Z",
        "session_key": 9999,
        "driver_number": 1,
        "speed": 55.0,
        "throttle": 35.0,
        "brake": 0.0,
        "gear": 3,
        "rpm": 6100.0,
        "source": "simulation",
        "zone": "pit_lane",
    },
    # === Driver 1 – near-threshold reading → DEFER ===
    {
        "timestamp": "2024-05-26T14:35:00Z",
        "session_key": 9999,
        "driver_number": 1,
        "speed": 59.5,
        "throttle": 42.0,
        "brake": 0.0,
        "gear": 3,
        "rpm": 7100.0,
        "source": "simulation",
        "zone": "pit_lane",
    },
    # === Driver 4 (NOR) – clear breach, well above limit ===
    {
        "timestamp": "2024-05-26T14:31:00Z",
        "session_key": 9999,
        "driver_number": 4,
        "speed": 67.8,
        "throttle": 55.0,
        "brake": 0.0,
        "gear": 3,
        "rpm": 8200.0,
        "source": "simulation",
        "zone": "pit_lane",
    },
    # === Driver 4 – normal passage, clearly safe ===
    {
        "timestamp": "2024-05-26T14:36:00Z",
        "session_key": 9999,
        "driver_number": 4,
        "speed": 42.0,
        "throttle": 20.0,
        "brake": 10.0,
        "gear": 2,
        "rpm": 4500.0,
        "source": "simulation",
        "zone": "pit_lane",
    },
    # === Driver 16 (LEC) – marginal breach, just over ===
    {
        "timestamp": "2024-05-26T14:32:00Z",
        "session_key": 9999,
        "driver_number": 16,
        "speed": 61.2,
        "throttle": 45.0,
        "brake": 0.0,
        "gear": 3,
        "rpm": 7400.0,
        "source": "simulation",
        "zone": "pit_lane",
    },
    # === Driver 16 – pit exit, not evaluated ===
    {
        "timestamp": "2024-05-26T14:33:00Z",
        "session_key": 9999,
        "driver_number": 16,
        "speed": 120.0,
        "throttle": 80.0,
        "brake": 0.0,
        "gear": 5,
        "rpm": 10200.0,
        "source": "simulation",
        "zone": "pit_exit",
    },
    # === Driver 44 (HAM) – another CLEAR passage ===
    {
        "timestamp": "2024-05-26T14:34:00Z",
        "session_key": 9999,
        "driver_number": 44,
        "speed": 51.3,
        "throttle": 28.0,
        "brake": 5.0,
        "gear": 2,
        "rpm": 5500.0,
        "source": "simulation",
        "zone": "pit_lane",
    },
    # === Driver 44 – on-track, not in pit ===
    {
        "timestamp": "2024-05-26T14:37:00Z",
        "session_key": 9999,
        "driver_number": 44,
        "speed": 295.0,
        "throttle": 100.0,
        "brake": 0.0,
        "gear": 8,
        "rpm": 11800.0,
        "source": "simulation",
        "zone": "sector_1",
    },
]


SAMPLE_TIMING: list[dict] = [
    # Simulated timing-loop observations for integrity checks.
    # These are NOT real FIA timing-loop data.
    {
        "timestamp": "2024-05-26T14:30:05Z",
        "session_key": 9999,
        "driver_number": 1,
        "sector": "pit_lane",
        "timing_speed": 55.5,
        "source": "simulation",
        "reference_speed": 55.0,
    },
    {
        "timestamp": "2024-05-26T14:31:00Z",
        "session_key": 9999,
        "driver_number": 4,
        "sector": "pit_lane",
        "timing_speed": 68.2,
        "source": "simulation",
        "reference_speed": 67.8,
    },
    {
        "timestamp": "2024-05-26T14:32:00Z",
        "session_key": 9999,
        "driver_number": 16,
        "sector": "pit_lane",
        "timing_speed": 61.5,
        "source": "simulation",
        "reference_speed": 61.2,
    },
    {
        "timestamp": "2024-05-26T14:34:00Z",
        "session_key": 9999,
        "driver_number": 44,
        "sector": "pit_lane",
        "timing_speed": 51.8,
        "source": "simulation",
        "reference_speed": 51.3,
    },
]


SAMPLE_RACE_CONTROL: list[dict] = [
    {
        "timestamp": "2024-05-26T14:00:00Z",
        "session_key": 9999,
        "category": "Flag",
        "message": "GREEN LIGHT - PIT LANE OPEN",
        "driver_number": None,
        "source": "simulation",
    },
    {
        "timestamp": "2024-05-26T14:31:30Z",
        "session_key": 9999,
        "category": "Penalty",
        "message": "CAR 4 (NOR) - PIT LANE SPEEDING - UNDER INVESTIGATION",
        "driver_number": 4,
        "source": "simulation",
    },
]
