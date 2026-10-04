"""
CALIBRE — Circuit and Track Geometry Model (§24, §26)
======================================================
Defines track characteristics, corner locations, DRS zones,
pit delta losses, and reference corner profiles.
"""
from __future__ import annotations

from typing import Dict, List, Optional
from pydantic import BaseModel, Field


class CornerGeometry(BaseModel):
    corner_number: int
    name: str
    distance_m: float
    corner_type: str  # "heavy_braking", "high_speed_sweeper", "hairpin", "chicane"
    ref_entry_speed_kph: float
    ref_apex_speed_kph: float
    ref_exit_speed_kph: float
    ref_braking_point_m: float
    ref_throttle_pickup_m: float


class CircuitModel:
    """Track model representing circuit parameters and corner benchmarks."""

    def __init__(self, name: str = "Silverstone Circuit", length_km: float = 5.891):
        self.name = name
        self.length_km = length_km
        self.length_m = length_km * 1000.0
        self.pit_lane_loss_green_s = 21.5
        self.pit_lane_loss_sc_s = 11.5

        # Key corners
        self.corners: Dict[str, CornerGeometry] = {
            "T1": CornerGeometry(
                corner_number=1, name="Abbey", distance_m=340.0,
                corner_type="high_speed_sweeper",
                ref_entry_speed_kph=295.0, ref_apex_speed_kph=280.0, ref_exit_speed_kph=290.0,
                ref_braking_point_m=300.0, ref_throttle_pickup_m=355.0
            ),
            "T3": CornerGeometry(
                corner_number=3, name="Village", distance_m=850.0,
                corner_type="heavy_braking",
                ref_entry_speed_kph=285.0, ref_apex_speed_kph=105.0, ref_exit_speed_kph=145.0,
                ref_braking_point_m=750.0, ref_throttle_pickup_m=865.0
            ),
            "T6": CornerGeometry(
                corner_number=6, name="Brooklands", distance_m=2150.0,
                corner_type="heavy_braking",
                ref_entry_speed_kph=305.0, ref_apex_speed_kph=145.0, ref_exit_speed_kph=180.0,
                ref_braking_point_m=2040.0, ref_throttle_pickup_m=2170.0
            ),
            "T7": CornerGeometry(
                corner_number=7, name="Luffield", distance_m=2450.0,
                corner_type="long_sweeper",
                ref_entry_speed_kph=210.0, ref_apex_speed_kph=108.0, ref_exit_speed_kph=165.0,
                ref_braking_point_m=2345.0, ref_throttle_pickup_m=2480.0
            ),
            "T9": CornerGeometry(
                corner_number=9, name="Copse", distance_m=3200.0,
                corner_type="high_speed_sweeper",
                ref_entry_speed_kph=300.0, ref_apex_speed_kph=265.0, ref_exit_speed_kph=285.0,
                ref_braking_point_m=3120.0, ref_throttle_pickup_m=3220.0
            ),
            "T15": CornerGeometry(
                corner_number=15, name="Stowe", distance_m=4650.0,
                corner_type="heavy_braking",
                ref_entry_speed_kph=315.0, ref_apex_speed_kph=185.0, ref_exit_speed_kph=220.0,
                ref_braking_point_m=4540.0, ref_throttle_pickup_m=4670.0
            ),
            "T16": CornerGeometry(
                corner_number=16, name="Vale", distance_m=5150.0,
                corner_type="heavy_braking",
                ref_entry_speed_kph=280.0, ref_apex_speed_kph=95.0, ref_exit_speed_kph=125.0,
                ref_braking_point_m=5050.0, ref_throttle_pickup_m=5165.0
            ),
        }

    def get_corner(self, corner_name: str) -> Optional[CornerGeometry]:
        return self.corners.get(corner_name)

    def get_all_corners(self) -> List[CornerGeometry]:
        return list(self.corners.values())
