"""CALIBRE Agent Processor Worker Service.

Runs the processing layer:
- Independent consumer groups per processor
- Consumes raw Kafka events (calibre.raw.*)
- Passes to specialized assurance agent adapters
- Publishes evidence events to calibre.evidence.<domain>
- Tracks health and metrics
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any, Optional

from calibre_core.contracts import RawSourceEnvelope, EvidenceEvent
from calibre_core.health import ComponentHealth, HealthStatus
from calibre_core.kafka_bus import CalibreProducer, CalibreConsumer
from calibre_core.topics import GROUP_PROCESSOR_PREFIX, all_standard_topics
from .agent_adapter import AgentProcessorAdapter, create_default_processors

logger = logging.getLogger("calibre.processors")


class ProcessorWorkerService:
    """Worker coordinating execution of independent agent processors connected to Kafka."""

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        processors: Optional[list[AgentProcessorAdapter]] = None,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.processors = processors or create_default_processors()
        self.producer = CalibreProducer(bootstrap_servers=bootstrap_servers, client_id="calibre-processor-worker")
        self.health = ComponentHealth(name="ProcessorWorkerService")
        self.health.set_dependency("kafka_producer", self.producer.is_connected)
        self.is_running = False
        self._threads: list[threading.Thread] = []

    def process_envelope_now(self, envelope: RawSourceEnvelope) -> list[EvidenceEvent]:
        """Synchronously process a raw envelope across all registered processors that accept it."""
        all_emitted: list[EvidenceEvent] = []
        for processor in self.processors:
            if processor.accepts(envelope):
                try:
                    events = processor.process(envelope)
                    for ev in events:
                        # Publish to domain evidence topic
                        self.producer.send(
                            topic=processor.output_topic,
                            value=ev,
                            correlation_id=ev.correlation_id,
                        )
                        all_emitted.append(ev)
                    self.health.record_activity(events_in=1, events_out=len(events))
                except Exception as exc:
                    self.health.record_activity(error=True)
                    self.producer.send_dlq(
                        component=processor.processor_id,
                        stage="AGENT_PROCESSING",
                        error_code="PROCESSING_ERROR",
                        error_message=str(exc),
                        raw_payload=envelope.to_dict(),
                        correlation_id=envelope.correlation_id,
                    )
        return all_emitted

    def _run_processor_consumer(self, processor: AgentProcessorAdapter) -> None:
        """Dedicated consumer thread per processor using independent consumer group."""
        group_id = f"{GROUP_PROCESSOR_PREFIX}{processor.domain}"
        
        # Determine raw topics of interest
        raw_topics = [t for t in all_standard_topics() if t.startswith("calibre.raw.")]
        
        consumer = CalibreConsumer(
            *raw_topics,
            group_id=group_id,
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset="earliest",
            consumer_timeout_ms=500,
        )

        logger.info(
            "Processor %s worker loop started (group=%s) on %d raw topics",
            processor.processor_id, group_id, len(raw_topics)
        )

        while self.is_running:
            try:
                messages = consumer.poll_messages(timeout_ms=500, max_records=20)
                for msg in messages:
                    try:
                        envelope = RawSourceEnvelope.from_dict(msg)
                    except Exception:
                        # Skip or send to DLQ if cannot parse RawSourceEnvelope
                        continue

                    if processor.accepts(envelope):
                        try:
                            events = processor.process(envelope)
                            for ev in events:
                                self.producer.send(
                                    topic=processor.output_topic,
                                    value=ev,
                                    correlation_id=ev.correlation_id,
                                )
                            self.health.record_activity(events_in=1, events_out=len(events))
                        except Exception as exc:
                            self.health.record_activity(error=True)
                            self.producer.send_dlq(
                                component=processor.processor_id,
                                stage="AGENT_PROCESSING",
                                error_code="PROCESSING_ERROR",
                                error_message=str(exc),
                                raw_payload=msg,
                                correlation_id=envelope.correlation_id,
                            )
            except Exception as exc:
                logger.error("Consumer loop error for %s: %s", processor.processor_id, exc)
                time.sleep(1.0)

        consumer.close()
        logger.info("Processor %s worker loop stopped", processor.processor_id)

    def start(self) -> None:
        """Start consumer threads for all registered processors."""
        self.is_running = True
        self.health.is_ready = True
        for processor in self.processors:
            t = threading.Thread(
                target=self._run_processor_consumer,
                args=(processor,),
                name=f"calibre-proc-{processor.domain}",
                daemon=True,
            )
            t.start()
            self._threads.append(t)
        logger.info("Started %d processor consumer threads", len(self._threads))

    def stop(self) -> None:
        """Stop all processor threads and close resources."""
        self.is_running = False
        for t in self._threads:
            t.join(timeout=2.0)
        self.producer.close()
        self.health.is_ready = False
        logger.info("ProcessorWorkerService stopped")
