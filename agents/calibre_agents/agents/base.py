from __future__ import annotations

from typing import Any

from ..models import AgentOutput, EvidenceEvent, Finding, SourceRecord, stable_id


class BaseAgent:
    agent_id = "base"
    agent_version = "0.1.0"
    handled_feeds: set[str] = set()

    def accepts(self, record: SourceRecord) -> bool:
        return record.feed in self.handled_feeds

    def finding(
        self,
        record: SourceRecord,
        *,
        code: str,
        severity: str,
        message: str,
        details: dict[str, Any] | None = None,
        limitations: list[str] | None = None,
    ) -> Finding:
        scope = {
            "year": record.context.get("year"),
            "meeting": record.context.get("meeting"),
            "session": record.context.get("session"),
            "driver_code": record.driver_code,
            "lap_number": record.lap_number,
        }
        finding_id = stable_id(self.agent_id, record.record_id, code, details or {})
        return Finding(
            finding_id=finding_id,
            agent_id=self.agent_id,
            code=code,
            severity=severity,
            message=message,
            scope={k: v for k, v in scope.items() if v is not None},
            evidence_record_ids=[record.record_id],
            details=details or {},
            limitations=limitations or [],
        )

    def event(
        self,
        record: SourceRecord,
        *,
        event_type: str,
        evidence: dict[str, Any],
        findings: list[Finding] | None = None,
        provenance_extra: dict[str, Any] | None = None,
    ) -> EvidenceEvent:
        scope = {
            "year": record.context.get("year"),
            "meeting": record.context.get("meeting"),
            "session": record.context.get("session"),
            "driver_code": record.driver_code,
            "driver_number": record.driver_number,
            "lap_number": record.lap_number,
        }
        provenance = dict(record.provenance)
        provenance.update(provenance_extra or {})
        return EvidenceEvent(
            event_id=stable_id(self.agent_id, record.record_id, event_type, evidence),
            schema_version="1.0",
            agent_id=self.agent_id,
            agent_version=self.agent_version,
            event_type=event_type,
            provider=record.provider,
            feed=record.feed,
            source_record_id=record.record_id,
            source_timestamp=record.source_timestamp,
            session_time_s=record.session_time_s,
            time_semantics=record.time_semantics,
            scope={k: v for k, v in scope.items() if v is not None},
            evidence=evidence,
            findings=[f.to_dict() for f in (findings or [])],
            provenance=provenance,
        )

    def process(self, record: SourceRecord) -> AgentOutput:
        return AgentOutput()
