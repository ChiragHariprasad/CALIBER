"""
CALIBRE — Driver Radio & Tactical Suggestions Engine
====================================================
Transforms raw analytical findings, strategy optimizer outputs, racing-line
micro-deltas, and safety car triggers into concise, authentic Formula 1
team-radio communications.

Message Categories:
  - BOX: Urgent pit call or window notification ("Box box, box box, pit confirm")
  - DELTA: Safety Car / VSC delta maintenance ("Safety car deployed, delta positive")
  - BRAKE / LINE: Micro-sector braking and line correction from LineAgent
  - TYRE: Compound degradation, thermal warnings, and management targets
  - TRAFFIC: Undercut/overcut warnings and dirty-air gaps
  - VERDICT: Steward investigations or pit speed compliance
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

from calibre.live.schemas.events import (
    CalibreEvent,
    DataOrigin,
    DataQuality,
    EventType,
    ValidationState,
)

logger = logging.getLogger(__name__)


class RadioPriority(str, Enum):
    CRITICAL = "CRITICAL"   # Urgent, safety-critical, immediate driver reaction needed
    HIGH = "HIGH"           # Immediate tactical action (Box call, Push lap)
    MEDIUM = "MEDIUM"       # Sector delta, tyre management advice
    LOW = "LOW"             # Background strategic update, weather forecast


class RadioCategory(str, Enum):
    BOX = "BOX"
    DELTA = "DELTA"
    BRAKE = "BRAKE"
    TYRE = "TYRE"
    TRAFFIC = "TRAFFIC"
    VERDICT = "VERDICT"
    WEATHER = "WEATHER"
    OVERTAKE = "OVERTAKE"


@dataclass
class RadioMessage:
    message_id: str
    target_driver: str
    sender: str              # "Race Engineer", "Strategy Desk", "Performance Eng"
    category: RadioCategory
    priority: RadioPriority
    message: str             # Spoken text
    lap_number: int
    timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "message_id": self.message_id,
            "target_driver": self.target_driver,
            "sender": self.sender,
            "category": self.category.value,
            "priority": self.priority.value,
            "message": self.message,
            "lap_number": self.lap_number,
            "timestamp": self.timestamp,
            "metadata": self.metadata,
        }


class DriverRadioEngine:
    """
    Synthesizes multi-agent telemetry analysis into authentic F1 driver radio messages.
    """

    def __init__(self, target_driver: str = "VER"):
        self.target_driver = target_driver
        self._history: List[RadioMessage] = []
        self._last_message_ts: Dict[str, float] = {}
        self._counter = 0

    def generate_pit_call(
        self,
        lap_number: int,
        target_compound: str = "HARD",
        urgent: bool = False,
    ) -> RadioMessage:
        """Issues an official pit instruction."""
        msg_text = (
            f"Box box, box this lap. Box for {target_compound.capitalize()}. Confirm on pit button."
            if urgent
            else f"Box at the end of this lap. Fitting {target_compound.capitalize()} tyres."
        )
        msg = RadioMessage(
            message_id=self._next_id("BOX"),
            target_driver=self.target_driver,
            sender="Race Engineer",
            category=RadioCategory.BOX,
            priority=RadioPriority.CRITICAL if urgent else RadioPriority.HIGH,
            message=msg_text,
            lap_number=lap_number,
            metadata={"compound": target_compound, "urgent": urgent},
        )
        self._record(msg)
        return msg

    def generate_sector_advice(
        self,
        lap_number: int,
        sector: int,
        delta_s: float,
        turn_number: Optional[int] = None,
        apex_delta_kph: Optional[float] = None,
    ) -> RadioMessage:
        """Performance engineer telemetry advice based on racing line agent."""
        if turn_number and apex_delta_kph:
            text = (
                f"We are losing {abs(delta_s):.2f}s in Sector {sector}. "
                f"Apex speed is {abs(apex_delta_kph):.0f} kph down at Turn {turn_number}. Carry more entry speed."
            )
        elif turn_number:
            text = (
                f"Sector {sector} loss of {abs(delta_s):.2f}s. "
                f"Braking 15 meters too early into Turn {turn_number}."
            )
        else:
            text = f"Pace note: Sector {sector} delta is +{abs(delta_s):.2f}s vs reference lap. Focus on exit traction."

        msg = RadioMessage(
            message_id=self._next_id("LINE"),
            target_driver=self.target_driver,
            sender="Performance Eng",
            category=RadioCategory.BRAKE,
            priority=RadioPriority.MEDIUM,
            message=text,
            lap_number=lap_number,
            metadata={"sector": sector, "delta_s": delta_s, "turn": turn_number},
        )
        self._record(msg)
        return msg

    def generate_tyre_management(
        self,
        lap_number: int,
        compound: str,
        wear_pct: float,
        overheating_axle: Optional[str] = None,
    ) -> RadioMessage:
        """Tyre degradation and thermal guidance."""
        if overheating_axle:
            text = (
                f"Tyre temperatures rising on the {overheating_axle} axle. "
                f"Manage slip angles in high-speed corners. Stint wear at {wear_pct:.0f}%."
            )
        else:
            text = (
                f"Degradation on the {compound.lower()} tyres is within target. "
                f"Wear at {wear_pct:.0f}%. We can extend this stint by 3 laps."
            )

        msg = RadioMessage(
            message_id=self._next_id("TYRE"),
            target_driver=self.target_driver,
            sender="Strategy Desk",
            category=RadioCategory.TYRE,
            priority=RadioPriority.MEDIUM,
            message=text,
            lap_number=lap_number,
            metadata={"wear_pct": wear_pct, "compound": compound},
        )
        self._record(msg)
        return msg

    def generate_safety_car_directive(
        self,
        track_status: str,
        delta_target_s: float = 1.2,
        lap_number: int = 0,
    ) -> RadioMessage:
        """Directives for SC / VSC conditions."""
        if "VSC" in track_status:
            text = f"Virtual Safety Car deployed. Maintain positive delta. Target delta is +{delta_target_s:.1f}s."
            priority = RadioPriority.CRITICAL
        elif "SAFETY" in track_status or track_status == "SC":
            text = "Safety Car deployed. Keep delta positive. Watch for cars slowing on pit entry."
            priority = RadioPriority.CRITICAL
        else:
            text = "Green flag! Track clear, race back on. Deploy mode 2."
            priority = RadioPriority.HIGH

        msg = RadioMessage(
            message_id=self._next_id("DELTA"),
            target_driver=self.target_driver,
            sender="Race Engineer",
            category=RadioCategory.DELTA,
            priority=priority,
            message=text,
            lap_number=lap_number,
            metadata={"track_status": track_status, "delta_target": delta_target_s},
        )
        self._record(msg)
        return msg

    def generate_verdict_notification(
        self,
        driver_number: str,
        verdict: str,
        lap_number: int,
        rule: str = "pit_lane_speed",
    ) -> RadioMessage:
        """Communications regarding steward and FIA rule evaluations."""
        if verdict == "BREACH":
            text = f"Warning: Pit lane speed violation flagged on Car #{driver_number}. Expect investigation."
            priority = RadioPriority.HIGH
        elif verdict == "DEFER":
            text = f"Notice: Pit speed reading for Car #{driver_number} disputed by sensor discrepancy. Deferred to telemetry review."
            priority = RadioPriority.MEDIUM
        else:
            text = f"Pit speed check clear on Car #{driver_number}."
            priority = RadioPriority.LOW

        msg = RadioMessage(
            message_id=self._next_id("VERDICT"),
            target_driver=self.target_driver,
            sender="Race Engineer",
            category=RadioCategory.VERDICT,
            priority=priority,
            message=text,
            lap_number=lap_number,
            metadata={"driver_number": driver_number, "verdict": verdict, "rule": rule},
        )
        self._record(msg)
        return msg

    def to_canonical_event(self, msg: RadioMessage, source_id: str = "radio_engine") -> CalibreEvent:
        """Wraps radio message into a canonical CalibreEvent envelope."""
        ev = CalibreEvent(
            event_type=EventType.RECOMMENDATION,
            source_id=source_id,
            operating_mode="LIVE",
            source_timestamp=msg.timestamp,
            driver=msg.target_driver,
            data_quality=DataQuality.HIGH,
            data=msg.to_dict(),
            field_origins={"message": DataOrigin.DERIVED.value},
        )
        ev.compute_dedup_key()
        return ev

    def _next_id(self, prefix: str) -> str:
        self._counter += 1
        return f"RAD-{prefix}-{self._counter:04d}"

    def _record(self, msg: RadioMessage) -> None:
        self._history.append(msg)
        logger.info("[%s -> %s] (%s) %s", msg.sender, msg.target_driver, msg.priority.value, msg.message)

    def get_recent_messages(self, limit: int = 15) -> List[Dict[str, Any]]:
        return [m.to_dict() for m in self._history[-limit:]]
