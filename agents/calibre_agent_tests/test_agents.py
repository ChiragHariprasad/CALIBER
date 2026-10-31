from __future__ import annotations

import json
import unittest
from pathlib import Path

from calibre_agents.models import SourceRecord
from calibre_agents.normalization import (
    infer_context_from_filename,
    infer_feed_from_filename,
    normalize_payload,
    payload_to_rows,
)
from calibre_agents.runner import AgentManager, build_lap_start_lookup
from calibre_agents.agents.vehicle_twin import VehicleTwinSensorAgent
from calibre_agents.agents.incident_track import IncidentTrackAgent
from calibre_agents.agents.environment import EnvironmentalContextAgent
from calibre_agents.agents.system_integrity import VehicleElectronicsSystemIntegrityAgent
from calibre_agents.agents.timing_session import RaceTimingSessionAgent
from calibre_agents.live_bridge import LiveAgentBridge


class NormalizationTests(unittest.TestCase):
    def test_openf1_style_row_array(self):
        payload = [{"date": "2025-01-01T00:00:00Z", "speed": 300}]
        self.assertEqual(payload_to_rows(payload), payload)

    def test_tracing_weather_columnar(self):
        payload = {"wT": [1.0, 2.0], "wAT": [20.0, 21.0], "wR": [False, True]}
        self.assertEqual(payload_to_rows(payload), [
            {"wT": 1.0, "wAT": 20.0, "wR": False},
            {"wT": 2.0, "wAT": 21.0, "wR": True},
        ])

    def test_nested_telemetry_columnar(self):
        payload = {"tel": {"time": [0.0, 0.2], "speed": [100, 101]}}
        self.assertEqual(payload_to_rows(payload), [
            {"time": 0.0, "speed": 100},
            {"time": 0.2, "speed": 101},
        ])

    def test_corners_kept_as_static_geometry_object(self):
        payload = {"CornerNumber": [1, 2], "X": [10.0, 20.0], "Y": [4.0, 8.0]}
        self.assertEqual(payload_to_rows(payload), [payload])

    def test_raw_filename_feed_and_context(self):
        name = "TRACINGINSIGHTS_2026_Bahrain_Grand_Prix_Race_52_tel_20261031T180512Z.json"
        self.assertEqual(infer_feed_from_filename(name), "52_tel")
        self.assertEqual(infer_context_from_filename(name), {
            "year": 2026,
            "meeting": "Bahrain Grand Prix",
            "session": "Race",
        })

    def test_sprint_qualifying_context(self):
        name = "TRACINGINSIGHTS_2025_Sao_Paulo_Grand_Prix_Sprint_Qualifying_weather_20251031T000000Z.json"
        context = infer_context_from_filename(name)
        self.assertEqual(context["session"], "Sprint Qualifying")

    def test_laptimes_time_semantics_and_lap_start_lookup(self):
        payload = [
            {"drv": "VER", "lap": 52, "time": 99.5, "sesT": 5200.0, "lST": 5100.5}
        ]
        lookup = build_lap_start_lookup(payload)
        self.assertEqual(lookup[("VER", 52)], 5100.5)
        records = normalize_payload(
            provider="TRACINGINSIGHTS",
            feed="session_laptimes",
            payload=payload,
        )
        self.assertEqual(records[0].session_time_s, 5200.0)
        self.assertEqual(records[0].time_semantics, "session_elapsed_at_lap_end")

    def test_lap_duration_is_never_promoted_to_event_timestamp(self):
        records = normalize_payload(
            provider="TRACINGINSIGHTS",
            feed="session_laptimes",
            payload=[{"drv": "VER", "lap": 1, "time": 98.7}],
        )
        self.assertIsNone(records[0].source_timestamp)
        self.assertIsNone(records[0].session_time_s)
        self.assertEqual(records[0].time_semantics, "lap_duration_only")

    def test_telemetry_time_is_lap_relative_without_mapping(self):
        payload = {"tel": {"time": [0.0], "speed": [104.8], "dataKey": ["2026-Bahrain Grand Prix-Race-VER-52"]}}
        records = normalize_payload(
            provider="TRACINGINSIGHTS", feed="52_tel", payload=payload,
        )
        self.assertEqual(records[0].driver_code, "VER")
        self.assertEqual(records[0].lap_number, 52)
        self.assertIsNone(records[0].session_time_s)
        self.assertEqual(records[0].time_semantics, "lap_relative_seconds")


class AgentTests(unittest.TestCase):
    def test_vehicle_agent_maps_sample_and_labels_derived_fields(self):
        agent = VehicleTwinSensorAgent()
        record = SourceRecord(
            record_id="r1", provider="TRACINGINSIGHTS", feed="52_tel",
            payload={
                "time": 0.0, "rpm": 9409, "speed": 104.875,
                "gear": 2, "throttle": 0.937, "brake": 0,
                "drs": 0, "distance": -0.003, "x": -580.8,
                "y": -2379.2, "z": 411, "acc_x": -2.31,
            },
            context={"year": 2026, "meeting": "Bahrain Grand Prix", "session": "Race"},
            source_timestamp=0.0, time_semantics="lap_relative_seconds",
            driver_code="VER", lap_number=52,
        )
        output = agent.process(record)
        self.assertEqual(len(output.events), 1)
        ev = output.events[0].to_dict()
        self.assertEqual(ev["event_type"], "vehicle_telemetry_sample_assessed")
        self.assertEqual(ev["evidence"]["measurement_semantics"]["x_y_z"], "interpolated_position_export")
        self.assertFalse(ev["provenance"]["raw_ecu_sensor_access"])

    def test_vehicle_agent_flags_non_monotonic_time(self):
        agent = VehicleTwinSensorAgent()
        base = dict(provider="TRACINGINSIGHTS", feed="1_tel", context={"session": "Race"}, driver_code="VER", lap_number=1)
        first = SourceRecord(record_id="1", payload={"time": 1.0, "speed": 100}, source_timestamp=1.0, **base)
        second = SourceRecord(record_id="2", payload={"time": 0.5, "speed": 100}, source_timestamp=0.5, **base)
        agent.process(first)
        out = agent.process(second)
        self.assertIn("TELEMETRY_TIME_NOT_MONOTONIC", [f.code for f in out.findings])

    def test_incident_agent_classifies_race_control_text(self):
        agent = IncidentTrackAgent()
        record = SourceRecord(
            record_id="rcm1", provider="TRACINGINSIGHTS", feed="rcm",
            payload={"time": "2025-10-19T19:06:51Z", "cat": "Other", "msg": "TURN 12 INCIDENT INVOLVING CARS 23 (ALB) AND 5 (BOR) NOTED - CAUSING A COLLISION", "lap": 2},
            source_timestamp="2025-10-19T19:06:51Z",
        )
        out = agent.process(record)
        self.assertEqual(out.events[0].event_type, "incident_notice")
        self.assertIn("RACE_CONTROL_INCIDENT_NOTICE", [f.code for f in out.findings])

    def test_environment_agent_normalises_weather(self):
        agent = EnvironmentalContextAgent()
        record = SourceRecord(
            record_id="w1", provider="TRACINGINSIGHTS", feed="weather",
            payload={"wT": 3.1, "wAT": 20.1, "wTT": 44.0, "wH": 73.2, "wP": 1015.0, "wR": False, "wWD": 180, "wWS": 2.0},
            source_timestamp=3.1,
        )
        out = agent.process(record)
        evidence = out.events[0].evidence
        self.assertEqual(evidence["air_temperature_c"], 20.1)
        self.assertEqual(evidence["rainfall"], False)

    def test_system_agent_does_not_claim_hidden_ecu_fault(self):
        agent = VehicleElectronicsSystemIntegrityAgent()
        record = SourceRecord(
            record_id="t1", provider="PITWALL_F1_SIGNALR_FREE", feed="cardata.z",
            payload={"Speed": 220}, source_timestamp="2026-11-01T12:00:00Z",
        )
        out = agent.process(record)
        self.assertIn("INTERNAL_ELECTRONICS_NOT_OBSERVABLE_IN_PUBLIC_FEED", [f.code for f in out.findings])
        self.assertIn("not evidence of an electronic fault", out.findings[0].limitations[0])

    def test_timing_agent_preserves_distinct_lap_and_session_times(self):
        agent = RaceTimingSessionAgent()
        record = SourceRecord(
            record_id="lap1", provider="TRACINGINSIGHTS", feed="session_laptimes",
            payload={"drv": "VER", "dNum": "1", "lap": 1, "time": 99.5,
                     "sesT": 3521.484, "lST": 3404.299, "s1": 27.0,
                     "s2": 39.0, "s3": 33.5, "pos": 1, "compound": "HARD"},
            source_timestamp=3521.484, session_time_s=3521.484,
            time_semantics="session_elapsed_at_lap_end",
            driver_code="VER", driver_number="1", lap_number=1,
        )
        output = agent.process(record)
        evidence = output.events[0].evidence
        self.assertEqual(evidence["lap_duration_s"], 99.5)
        self.assertEqual(evidence["session_elapsed_at_lap_end_s"], 3521.484)
        self.assertEqual(evidence["session_elapsed_at_lap_start_s"], 3404.299)

    def test_live_timing_agent_splits_driver_lines(self):
        agent = RaceTimingSessionAgent()
        record = SourceRecord(
            record_id="td1", provider="PITWALL_F1_SIGNALR_FREE", feed="timingdata",
            payload={"Lines": {"1": {"NumberOfLaps": 12, "LastLapTime": {"Value": "1:30.000"}},
                                "4": {"NumberOfLaps": 11}}},
            source_timestamp="provider-ts",
        )
        output = agent.process(record)
        self.assertEqual(len(output.events), 2)
        self.assertEqual(output.events[0].scope["driver_number"], "1")

    def test_live_race_control_messages_wrapper_is_unpacked(self):
        agent = IncidentTrackAgent()
        record = SourceRecord(
            record_id="live-rcm", provider="PITWALL_F1_SIGNALR_FREE", feed="racecontrolmessages",
            payload={"Messages": {"1": {"Category": "Other", "Message": "INCIDENT INVOLVING CARS 23 AND 5"},
                                  "2": {"Category": "Flag", "Message": "DOUBLE YELLOW", "Flag": "DOUBLE YELLOW"}}},
            source_timestamp="provider-ts",
        )
        output = agent.process(record)
        self.assertEqual(len(output.events), 2)
        self.assertEqual(output.events[0].event_type, "incident_notice")
        self.assertEqual(output.events[1].event_type, "track_state_or_flag")

    def test_live_bridge_accepts_pitwall_callback_signature(self):
        captured = []
        bridge = LiveAgentBridge(output_sink=captured.append, context={"mode": "live", "session": "Race"})
        bridge.on_event(
            "RaceControlMessages",
            {"Messages": {"1": {"Category": "Other", "Message": "INCIDENT INVOLVING CARS 23 AND 5"}}},
            "provider-time-1",
        )
        bridge.on_event(
            "TimingData",
            {"Lines": {"1": {"NumberOfLaps": 3}}},
            "provider-time-2",
        )
        self.assertEqual(bridge.stats()["source_records_seen"], 2)
        self.assertGreaterEqual(bridge.stats()["agent_events_emitted"], 4)
        self.assertTrue(any(event["agent_id"] == "incident_and_track_state_assurance" for event in captured))
        self.assertTrue(any(event["agent_id"] == "race_timing_and_session_evidence" for event in captured))

    def test_manager_fans_out_to_domain_and_integrity_agents(self):
        manager = AgentManager()
        record = SourceRecord(
            record_id="weather1", provider="TRACINGINSIGHTS", feed="weather",
            payload={"wT": 1.0, "wAT": 20.0, "wTT": 35.0, "wH": 50.0}, source_timestamp=1.0,
        )
        output = manager.process(record)
        ids = {event.agent_id for event in output.events}
        self.assertIn("environmental_context_assurance", ids)
        self.assertIn("vehicle_electronics_and_system_integrity", ids)


if __name__ == "__main__":
    unittest.main(verbosity=2)
