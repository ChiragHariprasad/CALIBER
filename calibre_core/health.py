"""CALIBRE Health and Observability Interface."""
from __future__ import annotations

import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional


class HealthStatus(str, Enum):
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    UNHEALTHY = "UNHEALTHY"
    UNKNOWN = "UNKNOWN"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


@dataclass
class ComponentHealth:
    """Standardized health structure for all CALIBRE services."""
    name: str
    status: HealthStatus = HealthStatus.UNKNOWN
    is_live: bool = True
    is_ready: bool = False
    dependencies: dict[str, bool] = field(default_factory=dict)
    details: dict[str, Any] = field(default_factory=dict)
    last_activity_utc: Optional[str] = None
    events_in: int = 0
    events_out: int = 0
    errors_count: int = 0
    updated_at_utc: str = field(default_factory=utc_now)

    def record_activity(self, events_in: int = 0, events_out: int = 0, error: bool = False) -> None:
        self.last_activity_utc = utc_now()
        self.events_in += events_in
        self.events_out += events_out
        if error:
            self.errors_count += 1
        self.updated_at_utc = utc_now()

    def set_dependency(self, dep_name: str, available: bool) -> None:
        self.dependencies[dep_name] = available
        self._evaluate()

    def _evaluate(self) -> None:
        all_deps = all(self.dependencies.values()) if self.dependencies else True
        if self.is_live and self.is_ready and all_deps:
            self.status = HealthStatus.HEALTHY
        elif self.is_live and (not all_deps or not self.is_ready):
            self.status = HealthStatus.DEGRADED
        elif not self.is_live:
            self.status = HealthStatus.UNHEALTHY
        else:
            self.status = HealthStatus.UNKNOWN
        self.updated_at_utc = utc_now()

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["status"] = self.status.value
        return data


@dataclass
class SystemHealthReport:
    """Aggregated health report across the pipeline."""
    system: str = "CALIBRE"
    overall_status: HealthStatus = HealthStatus.UNKNOWN
    generated_at_utc: str = field(default_factory=utc_now)
    components: dict[str, dict[str, Any]] = field(default_factory=dict)

    @classmethod
    def from_components(cls, components: list[ComponentHealth]) -> "SystemHealthReport":
        comp_dict = {c.name: c.to_dict() for c in components}
        statuses = [c.status for c in components]
        
        if all(s == HealthStatus.HEALTHY for s in statuses):
            overall = HealthStatus.HEALTHY
        elif any(s == HealthStatus.UNHEALTHY for s in statuses):
            overall = HealthStatus.UNHEALTHY
        elif any(s == HealthStatus.DEGRADED for s in statuses):
            overall = HealthStatus.DEGRADED
        else:
            overall = HealthStatus.UNKNOWN
            
        return cls(
            overall_status=overall,
            components=comp_dict,
            generated_at_utc=utc_now(),
        )

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["overall_status"] = self.overall_status.value
        return data

