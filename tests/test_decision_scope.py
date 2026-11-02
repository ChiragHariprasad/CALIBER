from pathlib import Path

import pytest

from sia_core.contracts.parser import load_contract_file
from sia_core.contracts.scope import DecisionScopeError, decision_scope_key


DEFS = Path(__file__).resolve().parents[1] / "sia_core" / "contracts" / "definitions"


def test_driver_lap_scope_separates_driver_and_lap():
    contract = load_contract_file(DEFS / "dec_01_lap_time_consistency.yaml")
    scope_a = decision_scope_key(contract, {"session_id": "race", "driver_number": 1, "lap_number": 12})
    scope_b = decision_scope_key(contract, {"session_id": "race", "driver_number": 1, "lap_number": 13})
    scope_c = decision_scope_key(contract, {"session_id": "race", "driver_number": 2, "lap_number": 12})
    assert scope_a == "race|1|lap12"
    assert len({scope_a, scope_b, scope_c}) == 3


def test_session_scope_ignores_incidental_driver_fields():
    contract = load_contract_file(DEFS / "dec_03_race_control_incident_timing.yaml")
    a = decision_scope_key(contract, {"session_id": "race", "driver_number": 1, "lap_number": 12})
    b = decision_scope_key(contract, {"session_id": "race", "driver_number": 2, "lap_number": 13})
    assert a == b == "race"


def test_missing_required_lap_scope_is_explicit_error():
    contract = load_contract_file(DEFS / "dec_01_lap_time_consistency.yaml")
    with pytest.raises(DecisionScopeError, match="MISSING_DECISION_SCOPE_FIELD:lap_number"):
        decision_scope_key(contract, {"session_id": "race", "driver_number": 1})


def test_weather_contract_preserves_distinct_measurement_selectors():
    contract = load_contract_file(DEFS / "dec_06_weather_strategy_advisory.yaml")
    assert contract.roles["track_weather"].input_selector.value_field == "track_temperature_c"
    assert contract.roles["air_weather"].input_selector.value_field == "air_temperature_c"


def test_sensor_contract_selects_rpm_and_does_not_claim_missing_sensor_envelope():
    contract = load_contract_file(DEFS / "dec_10_vehicle_sensor_anomaly.yaml")
    assert contract.roles["telemetry_sample"].input_selector.value_field == "rpm"
    assert contract.roles["sensor_envelope"].admissible_source_origins == ["VEHICLE_PHYSICAL_MODEL"]
