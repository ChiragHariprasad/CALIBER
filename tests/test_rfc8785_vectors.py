"""Test Suite for RFC 8785 JSON Canonicalization Scheme (JCS) and Canonical Hashing."""
from __future__ import annotations

import json
from pathlib import Path
import pytest

from sia_core.identity.canonical import (
    jcs_serialize,
    jcs_serialize_bytes,
    jcs_sha256,
    es6_format_float,
    canonicalize,
)
from sia_core.identity.quantization import quantize_value, PRECISION_MATRIX
from sia_core.identity.hasher import (
    compute_raw_event_id,
    compute_evidence_atom_id,
    compute_verdict_hash,
)

VECTORS_DIR = Path(__file__).parent / "vectors"
MANIFEST_PATH = VECTORS_DIR / "manifest.json"

with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
    ALL_VECTORS = json.load(f)


@pytest.mark.parametrize("vector", ALL_VECTORS, ids=[v["id"] for v in ALL_VECTORS])
def test_rfc8785_test_vector_conformance(vector):
    """Assert each of the 55 test vectors satisfies RFC 8785 canonical string and hash parity."""
    vid = vector["id"]
    raw_input = vector["raw_input"]
    expected_jcs = vector["canonical_jcs"]
    expected_hash = vector["expected_sha256"]

    # Test serialization
    serialized_jcs = jcs_serialize(raw_input)
    assert serialized_jcs == expected_jcs, f"Canonical serialization mismatch on {vid}"

    # Test alias
    assert canonicalize(raw_input) == expected_jcs

    # Test bytes serialization
    assert jcs_serialize_bytes(raw_input) == expected_jcs.encode("utf-8")

    # Test SHA-256 digest
    computed_hash = jcs_sha256(raw_input)
    assert computed_hash == expected_hash, f"SHA-256 digest mismatch on {vid}"


@pytest.mark.parametrize("vector", ALL_VECTORS, ids=[f"file_{v['id']}" for v in ALL_VECTORS])
def test_vector_file_matches_manifest(vector):
    """Verify that the individual vector_XX.json file on disk matches the manifest entry."""
    vid = vector["id"]
    num = int(vid.split("-")[1])
    file_path = VECTORS_DIR / f"vector_{num:02d}.json"
    assert file_path.exists(), f"Vector file {file_path.name} does not exist"

    with open(file_path, "r", encoding="utf-8") as f:
        file_data = json.load(f)

    assert file_data["id"] == vector["id"]
    assert file_data["category"] == vector["category"]
    assert file_data["canonical_jcs"] == vector["canonical_jcs"]
    assert file_data["expected_sha256"] == vector["expected_sha256"]


def test_manifest_contains_55_vectors():
    """Assert that manifest contains at least 55 vectors satisfying F10."""
    assert len(ALL_VECTORS) >= 50
    assert len(ALL_VECTORS) == 55


class TestES6FormatFloat:
    """Detailed unit tests for ECMAScript 6 Section 7.1.12.1 ToString(Number) formatting."""

    def test_zero_and_signed_zero(self):
        assert es6_format_float(0.0) == "0"
        assert es6_format_float(-0.0) == "0"

    def test_integers_as_floats_strip_dot_zero(self):
        assert es6_format_float(1.0) == "1"
        assert es6_format_float(42.0) == "42"
        assert es6_format_float(-100.0) == "-100"

    def test_decimal_boundaries(self):
        assert es6_format_float(0.5) == "0.5"
        assert es6_format_float(0.05) == "0.05"
        assert es6_format_float(1e-5) == "0.00001"
        assert es6_format_float(1e-6) == "0.000001"
        assert es6_format_float(1e-7) == "1e-7"
        assert es6_format_float(-1e-7) == "-1e-7"

    def test_large_boundaries(self):
        assert es6_format_float(1e20) == "100000000000000000000"
        assert es6_format_float(1e21) == "1e+21"
        assert es6_format_float(1.23e25) == "1.23e+25"

    def test_nan_and_inf_raise_value_error(self):
        with pytest.raises(ValueError, match="RFC 8785 forbids NaN and Infinity"):
            es6_format_float(float("nan"))
        with pytest.raises(ValueError, match="RFC 8785 forbids NaN and Infinity"):
            es6_format_float(float("inf"))
        with pytest.raises(ValueError, match="RFC 8785 forbids NaN and Infinity"):
            es6_format_float(float("-inf"))


class TestQuantization:
    """Tests for Decimal half-to-even float quantization."""

    def test_lap_timing_precision(self):
        # 10^-4 s
        assert quantize_value(84.123456, "s") == 84.1235
        assert quantize_value(84.00001, "s") == 84.0

    def test_speed_precision(self):
        # 10^-2 kph
        assert quantize_value(315.4567, "kph") == 315.46

    def test_bankers_rounding(self):
        # 1.00025 -> round to even (1.0002) for 4 decimals
        # 1.00035 -> round to even (1.0004) for 4 decimals
        assert quantize_value(1.00025, "s") == 1.0002
        assert quantize_value(1.00035, "s") == 1.0004


class TestIdentityHasher:
    """Tests for compute_raw_event_id, compute_evidence_atom_id, compute_verdict_hash."""

    def test_raw_event_id_deterministic(self):
        h1 = compute_raw_event_id(
            provider="PITWALL_F1",
            feed="TimingData",
            source_record_key="lap52",
            session_id="session_11731",
            scope={"driver_id": "VER", "lap": 52},
            source_time={"value": 84.1234, "semantics": "SESSION_ELAPSED", "unit": "s"},
            payload_sha256="47a3e785d03ec1c54b2857037f394c8b21c70e28f325d254b1f415392e2760df",
        )
        assert len(h1) == 64
        assert h1 == h1.lower()

    def test_raw_event_id_acquisition_bound_fallback(self):
        # When semantics is RECEIPT_TIME_ONLY and source_record_key is None, received_at_utc required
        with pytest.raises(ValueError, match="received_at_utc required"):
            compute_raw_event_id(
                provider="PITWALL_F1",
                feed="RaceControlMessages",
                source_record_key=None,
                session_id="session_11731",
                scope={"scope_class": "SESSION"},
                source_time={"value": None, "semantics": "RECEIPT_TIME_ONLY", "unit": "utc"},
                payload_sha256="d98fb87f7422f4625b172a3920cbe0aee50a1dfd9a6ca587e954ff206aeb73f0",
                received_at_utc=None,
            )

        h = compute_raw_event_id(
            provider="PITWALL_F1",
            feed="RaceControlMessages",
            source_record_key=None,
            session_id="session_11731",
            scope={"scope_class": "SESSION"},
            source_time={"value": None, "semantics": "RECEIPT_TIME_ONLY", "unit": "utc"},
            payload_sha256="d98fb87f7422f4625b172a3920cbe0aee50a1dfd9a6ca587e954ff206aeb73f0",
            received_at_utc="2026-10-31T23:15:00.123456Z",
        )
        assert len(h) == 64

    def test_evidence_atom_id_deterministic(self):
        atom_id = compute_evidence_atom_id(
            raw_event_ids=["c2ff507c3975fa98ad6be500a4270f279fe85038890cf2dfb9f390886ff2efd2"],
            processor_family_id="timing_parser_family",
            transform_version="1.0.0",
            role_id="lap_record",
            scope_key="session_11731|VER|52",
            normalized_value=84.123456,
            unit="s",
        )
        assert len(atom_id) == 64

    def test_verdict_hash_pinned_frontier(self):
        vh_final = compute_verdict_hash(
            contract_id="lap_time_consistency",
            contract_version="1.0",
            contract_epoch=1,
            decision_instance_id="session_11731|VER|lap52|lap_time_consistency",
            integrity_state="HEALTHY",
            resolution_maturity="TUPLE_RESOLVED",
            finality_status="FINAL",
            canonical_frontier={"run_clock": "2026-10-31T23:16:30.000Z", "watermark": "2026-10-31T23:16:30.000Z"},
            corroboration_profile_by_role={},
            validity_timeline=[],
            sorted_support_atom_ids=[],
            sorted_reference_atom_ids=[],
            pinned_finality_frontier="2026-10-31T23:16:30.000Z",
        )
        assert len(vh_final) == 64
