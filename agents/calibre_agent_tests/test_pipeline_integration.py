from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from run_agents import run_files


class PipelineIntegrationTests(unittest.TestCase):
    def test_session_timebase_and_agent_fanout_from_raw_archives(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            files = {
                "TRACINGINSIGHTS_2026_Bahrain_Grand_Prix_Race_session_info_20261031T180512Z.json": {
                    "t0_date": "2026-04-12T15:00:00.000000",
                    "session_start_time": 0,
                    "total_laps": 57,
                    "session_info": {"Name": "Race"},
                },
                "TRACINGINSIGHTS_2026_Bahrain_Grand_Prix_Race_session_laptimes_20261031T180513Z.json": [
                    {"drv": "VER", "dNum": "3", "lap": 52, "time": 99.5,
                     "sesT": 5200.0, "lST": 5100.5, "s1": 27.0,
                     "s2": 39.0, "s3": 33.5, "pos": 1, "compound": "HARD"}
                ],
                "TRACINGINSIGHTS_2026_Bahrain_Grand_Prix_Race_52_tel_20261031T180514Z.json": {
                    "tel": {
                        "time": [0.0, 0.113],
                        "rpm": [9409, 9285],
                        "speed": [104.875, 103.462],
                        "gear": [2, 2],
                        "throttle": [0.937, 0.231],
                        "brake": [0, 0],
                        "drs": [0, 0],
                        "distance": [-0.003, 3.235],
                        "dataKey": [
                            "2026-Bahrain Grand Prix-Race-VER-52",
                            "2026-Bahrain Grand Prix-Race-VER-52",
                        ],
                    }
                },
                "TRACINGINSIGHTS_2026_Bahrain_Grand_Prix_Race_rcm_20261031T180515Z.json": [
                    {"time": "2026-04-12T15:03:00.000000Z", "cat": "Other",
                     "msg": "TURN 4 INCIDENT INVOLVING CARS 3 AND 5", "lap": 2}
                ],
            }
            paths = []
            for filename, payload in files.items():
                path = root / filename
                path.write_text(json.dumps(payload), encoding="utf-8")
                paths.append(path)

            result = run_files(paths, root / "output")
            telemetry = [e for e in result.events if e["event_type"] == "vehicle_telemetry_sample_assessed"]
            incidents = [e for e in result.events if e["event_type"] == "incident_notice"]
            timing = [e for e in result.events if e["event_type"] == "lap_timing_evidence"]

            self.assertEqual(len(telemetry), 2)
            self.assertAlmostEqual(telemetry[0]["session_time_s"], 5100.5)
            self.assertEqual(telemetry[0]["time_semantics"], "session_elapsed_from_lST_plus_lap_relative")
            self.assertEqual(telemetry[0]["provenance"]["lap_start_source"], "session_laptimes.lST")

            self.assertEqual(len(incidents), 1)
            self.assertAlmostEqual(incidents[0]["session_time_s"], 180.0, places=3)
            self.assertEqual(len(timing), 1)
            self.assertEqual(timing[0]["evidence"]["lap_duration_s"], 99.5)


if __name__ == "__main__":
    unittest.main(verbosity=2)
