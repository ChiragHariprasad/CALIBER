from __future__ import annotations

from typing import Any, Callable

from .models import SourceRecord, utc_now, stable_id
from .runner import AgentManager


class LiveAgentBridge:
    """Adapter for Pitwall's `client.on_all(topic, callback)` contract.

    Pass `bridge.on_event` to Pitwall. It processes each callback synchronously
    and optionally forwards serialized agent outputs to a caller-supplied sink.
    The sink can later be replaced with a Kafka producer.
    """

    def __init__(
        self,
        manager: AgentManager | None = None,
        output_sink: Callable[[dict[str, Any]], None] | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        self.manager = manager or AgentManager()
        self.output_sink = output_sink
        self.context = context or {"mode": "live"}
        self.records_seen = 0
        self.events_emitted = 0

    def on_event(self, topic: str, data: Any, timestamp: Any) -> None:
        self.records_seen += 1
        payload = data if isinstance(data, dict) else {"value": data}
        feed = str(topic).lower()
        record = SourceRecord(
            record_id=stable_id("PITWALL", topic, timestamp, self.records_seen, payload),
            provider="PITWALL_F1_SIGNALR_FREE",
            feed=feed,
            payload=payload,
            context=dict(self.context),
            source_timestamp=timestamp,
            session_time_s=None,
            time_semantics="provider_timestamp_unconverted",
            received_at=utc_now(),
            provenance={
                "provider_timestamp_preserved": True,
                "pitwall_auto_merge_expected": True,
                "raw_delta_available_to_callback": False,
            },
        )
        output = self.manager.process(record)
        for event in output.events:
            serialised = event.to_dict()
            self.events_emitted += 1
            if self.output_sink:
                self.output_sink(serialised)

    def stats(self) -> dict[str, int]:
        return {
            "source_records_seen": self.records_seen,
            "agent_events_emitted": self.events_emitted,
        }
