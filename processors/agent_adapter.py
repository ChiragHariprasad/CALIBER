"""CALIBRE Agent Processor Adapter.

Integrates the existing calibre_agents into the asynchronous Kafka event streaming architecture.
Preserves correlation IDs, provenance, original payload, and produces versioned EvidenceEvents.
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any, Optional

# Ensure agents directory is on sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
AGENTS_DIR = BASE_DIR / "agents"
if str(AGENTS_DIR) not in sys.path:
    sys.path.insert(0, str(AGENTS_DIR))

try:
    from calibre_agents.models import SourceRecord, AgentOutput
    from calibre_agents.agents.vehicle_twin import VehicleTwinSensorAgent
    from calibre_agents.agents.incident_track import IncidentTrackAgent
    from calibre_agents.agents.environment import EnvironmentalContextAgent
    from calibre_agents.agents.timing_session import RaceTimingSessionAgent
    from calibre_agents.agents.system_integrity import VehicleElectronicsSystemIntegrityAgent
    CALIBRE_AGENTS_AVAILABLE = True
except ImportError as exc:
    CALIBRE_AGENTS_AVAILABLE = False
    logging.getLogger("calibre.agent_adapter").warning("calibre_agents not found: %s", exc)

from calibre_core.contracts import RawSourceEnvelope, EvidenceEvent, Finding, utc_now, stable_id
from calibre_core.topics import evidence_topic

logger = logging.getLogger("calibre.agent_adapter")


class AgentProcessorAdapter:
    """Wraps an existing or future agent into an independent Kafka-connected evidence processor."""

    def __init__(
        self,
        processor_id: str,
        domain: str,
        agent_instance: Any,
        version: str = "0.1.0",
        subscribed_feeds: Optional[set[str]] = None,
    ) -> None:
        self.processor_id = processor_id
        self.domain = domain
        self.agent = agent_instance
        self.version = version
        self.subscribed_feeds = subscribed_feeds or getattr(agent_instance, "handled_feeds", set())
        self.output_topic = evidence_topic(domain)
        self.processed_count = 0
        self.emitted_count = 0
        self.error_count = 0

    def accepts(self, envelope: RawSourceEnvelope) -> bool:
        """Determine if this processor handles the raw feed."""
        if not self.subscribed_feeds:
            return True  # System integrity / catch-all agent
        feed_lower = envelope.feed.lower()
        return any(subscribed in feed_lower or feed_lower in subscribed for subscribed in self.subscribed_feeds)

    def process(self, envelope: RawSourceEnvelope) -> list[EvidenceEvent]:
        """Convert RawSourceEnvelope -> SourceRecord -> AgentOutput -> list[EvidenceEvent].
        
        Strictly preserves the incoming correlation_id and lineage references.
        """
        self.processed_count += 1
        results: list[EvidenceEvent] = []

        try:
            # Map RawSourceEnvelope to existing SourceRecord
            payload_dict = envelope.payload if isinstance(envelope.payload, dict) else {"data": envelope.payload}
            source_rec = SourceRecord(
                record_id=envelope.event_id,
                provider=envelope.provider,
                feed=envelope.feed,
                payload=payload_dict,
                context=envelope.session or {},
                source_timestamp=envelope.source_time.get("value"),
                session_time_s=envelope.session_time_s,
                time_semantics=envelope.source_time.get("semantics", "unknown"),
                received_at=envelope.received_at_utc,
                provenance={
                    "run_id": envelope.run_id,
                    "source_mode": envelope.source_mode,
                    "payload_sha256": envelope.payload_sha256,
                    "correlation_id": envelope.correlation_id,
                },
            )

            # Invoke underlying agent if available
            agent_output = None
            if self.agent and hasattr(self.agent, "process"):
                try:
                    agent_output = self.agent.process(source_rec)
                except Exception as exc:
                    logger.warning("Underlying agent %s raised exception: %s. Using pass-through.", self.processor_id, exc)

            if agent_output and agent_output.events:
                # Transform each agent event to standard contract EvidenceEvent
                for ev in agent_output.events:
                    ev_dict = ev.to_dict() if hasattr(ev, "to_dict") else ev
                    evidence_event = EvidenceEvent(
                        event_id=stable_id(self.processor_id, envelope.event_id, ev_dict.get("event_type")),
                        correlation_id=envelope.correlation_id,
                        event_type=ev_dict.get("event_type", f"{self.domain}_evidence"),
                        domain=self.domain,
                        producer={
                            "processor_id": self.processor_id,
                            "version": self.version,
                            "build": "scaffold-v1",
                        },
                        produced_at_utc=utc_now(),
                        source_refs=[envelope.event_id],
                        session=envelope.session,
                        scope={**(envelope.scope or {}), **(ev_dict.get("scope") or {})},
                        time={
                            "source_value": envelope.source_time.get("value"),
                            "source_semantics": envelope.source_time.get("semantics"),
                            "session_time_s": envelope.session_time_s,
                            "event_time_utc": envelope.event_time_utc,
                        },
                        value_origin="PROCESSOR_ASSESSMENT" if ev_dict.get("findings") else "SOURCE_REPORTED",
                        evidence=ev_dict.get("evidence", payload_dict),
                        findings=ev_dict.get("findings", []),
                        limitations=ev_dict.get("limitations", []),
                        provenance={
                            "provider": envelope.provider,
                            "feed": envelope.feed,
                            "raw_event_id": envelope.event_id,
                            "correlation_id": envelope.correlation_id,
                            "processor": self.processor_id,
                        },
                    )
                    results.append(evidence_event)
            else:
                # Pass-through evidence event ensuring end-to-end data movement
                evidence_event = EvidenceEvent(
                    event_id=stable_id(self.processor_id, envelope.event_id, "passthrough"),
                    correlation_id=envelope.correlation_id,
                    event_type=f"{self.domain}_evidence_sample",
                    domain=self.domain,
                    producer={
                        "processor_id": self.processor_id,
                        "version": self.version,
                        "build": "scaffold-v1",
                    },
                    produced_at_utc=utc_now(),
                    source_refs=[envelope.event_id],
                    session=envelope.session,
                    scope=envelope.scope,
                    time={
                        "source_value": envelope.source_time.get("value"),
                        "source_semantics": envelope.source_time.get("semantics"),
                        "session_time_s": envelope.session_time_s,
                        "event_time_utc": envelope.event_time_utc,
                    },
                    value_origin="SOURCE_REPORTED",
                    evidence=payload_dict,
                    findings=[],
                    limitations=["PASS_THROUGH_EVIDENCE_SCAFFOLD"],
                    provenance={
                        "provider": envelope.provider,
                        "feed": envelope.feed,
                        "raw_event_id": envelope.event_id,
                        "correlation_id": envelope.correlation_id,
                        "processor": self.processor_id,
                    },
                )
                results.append(evidence_event)

            self.emitted_count += len(results)
            return results

        except Exception as exc:
            self.error_count += 1
            logger.error("Processor %s error processing event %s: %s", self.processor_id, envelope.event_id, exc)
            raise


def create_default_processors() -> list[AgentProcessorAdapter]:
    """Instantiate and wrap all 5 CALIBRE specialist assurance processors."""
    processors: list[AgentProcessorAdapter] = []

    if CALIBRE_AGENTS_AVAILABLE:
        # 1. Vehicle Twin & Sensor Assurance
        processors.append(
            AgentProcessorAdapter(
                processor_id="vehicle_twin_sensor_agent",
                domain="vehicle",
                agent_instance=VehicleTwinSensorAgent(),
                version="0.1.0",
                subscribed_feeds={"telemetry", "tel", "car_data", "location"},
            )
        )
        # 2. Incident & Track State Assurance
        processors.append(
            AgentProcessorAdapter(
                processor_id="incident_track_agent",
                domain="incident",
                agent_instance=IncidentTrackAgent(),
                version="0.1.0",
                subscribed_feeds={"rcm", "racecontrolmessages", "trackstatus", "race_control"},
            )
        )
        # 3. Environmental Context Agent
        processors.append(
            AgentProcessorAdapter(
                processor_id="environmental_context_agent",
                domain="environment",
                agent_instance=EnvironmentalContextAgent(),
                version="0.1.0",
                subscribed_feeds={"weather", "weatherdata", "weather_data"},
            )
        )
        # 4. Race Timing & Session Agent
        processors.append(
            AgentProcessorAdapter(
                processor_id="race_timing_session_agent",
                domain="timing",
                agent_instance=RaceTimingSessionAgent(),
                version="0.1.0",
                subscribed_feeds={
                    "session_laptimes", "laptimes", "laps", "pit", "session_info", "drivers",
                    "timingdata", "timingappdata", "timingstats", "driverlist",
                    "lapcount", "sessionstatus", "timing", "intervals", "position",
                },
            )
        )
        # 5. Vehicle Electronics & System Integrity
        processors.append(
            AgentProcessorAdapter(
                processor_id="vehicle_electronics_system_integrity_agent",
                domain="system",
                agent_instance=VehicleElectronicsSystemIntegrityAgent(),
                version="0.1.0",
                subscribed_feeds=set(),  # Subscribes to all feeds
            )
        )
    else:
        # Fallback dummy processors if agents not importable
        domains = ["vehicle", "incident", "environment", "timing", "system"]
        for d in domains:
            processors.append(
                AgentProcessorAdapter(
                    processor_id=f"calibre_{d}_processor",
                    domain=d,
                    agent_instance=None,
                    version="0.1.0",
                )
            )

    return processors

