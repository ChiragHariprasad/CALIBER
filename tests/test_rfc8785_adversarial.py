"""Adversarial Verification Script for RFC 8785 JCS, Quantization, and Hasher.

Empirical verification against:
1. ECMAScript 6 float formatting parity with Node.js across thousands of boundary values.
2. UTF-16 code units lexicographical key sorting (BMP vs Supplementary surrogate pairs).
3. JSON string escaping and RFC 8785 prohibition compliance.
4. Banker's rounding half-to-even quantization and precision matrix consistency.
5. Content identity hashing determinism and property invariants.
"""
from __future__ import annotations

import hashlib
import json
import math
import random
import subprocess
import sys
from decimal import Decimal, ROUND_HALF_EVEN
from pathlib import Path
from typing import Any, Dict, List, Tuple

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


def get_nodejs_float_strings(floats: List[float]) -> List[str]:
    """Invoke Node.js to get exact ECMAScript 6 Number.prototype.toString() representation."""
    js_code = "console.log(JSON.stringify(JSON.parse(process.argv[1]).map(x => x.toString())));"
    proc = subprocess.run(
        ["node", "-e", js_code, json.dumps(floats)],
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(proc.stdout)


class TestAdversarialUTF16KeyOrdering:
    """Adversarially challenge UTF-16 code unit ordering vs UTF-8 / code points."""

    def test_bmp_vs_supplementary_surrogate_pairs(self):
        """RFC 8785 §3.2.3: UTF-16 code units comparison means Supplementary characters
        (U+10000..U+10FFFF encoded as 0xD800..0xDBFF surrogates) must sort BEFORE
        high BMP characters (U+E000..U+FFFF).
        """
        # U+FFFF (code point 65535, UTF-16 code unit 0xFFFF)
        # U+10000 (code point 65536, UTF-16 high surrogate 0xD800, low surrogate 0xDC00)
        # In Python code point sorting: "\uFFFF" < "\U00010000" (65535 < 65536)
        # In UTF-16 code units: 0xD800 < 0xFFFF, so "\U00010000" < "\uFFFF"
        data = {"\uFFFF": "high_bmp", "\U00010000": "supp_linear_b"}
        canonical = jcs_serialize(data)
        # "\U00010000" must come first
        expected = '{"\U00010000":"supp_linear_b","\uFFFF":"high_bmp"}'
        assert canonical == expected

    def test_multiple_supplementary_planes(self):
        """Supplementary Plane 1 (SMP: U+10000..U+1FFFF), Plane 2 (SIP: U+20000..U+2FFFF),
        Plane 14 (SSP: U+E0000..U+EFFFF), Plane 16 (U+100000..U+10FFFF) vs BMP.
        """
        keys = [
            "\uFFFF",            # BMP high: 0xFFFF
            "\u0000",            # BMP low: 0x0000
            "\uE000",            # Private use BMP: 0xE000
            "\U00010000",        # Linear B (0xD800 0xDC00)
            "\U0001F3CE",        # Racing car emoji (0xD83C 0xDFCE)
            "\U00020000",        # CJK extension B (0xD840 0xDC00)
            "\U0010FFFF",        # Max Unicode (0xDBFF 0xDFFF)
            "\uD7FF",            # Just before surrogates: 0xD7FF
        ]
        shuffled = list(keys)
        random.seed(42)
        random.shuffle(shuffled)
        obj = {k: i for i, k in enumerate(shuffled)}

        canonical = jcs_serialize(obj)

        # Expected order based on UTF-16 code units:
        sorted_keys = sorted(keys, key=lambda k: k.encode("utf-16-be"))
        # Verify our test expectation matches Java/Node.js UTF-16 ordering
        for i in range(len(sorted_keys) - 1):
            k1 = sorted_keys[i].encode("utf-16-be")
            k2 = sorted_keys[i + 1].encode("utf-16-be")
            assert k1 < k2

        # Verify canonical string has keys in sorted_keys order
        parsed_order = []
        # Extract keys in order of appearance in canonical JSON
        # Since keys are JSON string escaped, we reconstruct the key list
        curr = json.loads(canonical)
        assert list(curr.keys()) == sorted_keys

    def test_prefix_sorting_adversarial(self):
        """Keys that share long common prefixes with differing trailing code units."""
        keys = [
            "telemetry_lap",
            "telemetry_lap_52",
            "telemetry_lap_5",
            "telemetry_lap_52_VER",
            "telemetry_lap_52_HAM",
            "telemetry_lap_\uFFFF",
            "telemetry_lap_\U0001F3CE",
            "telemetry_lap_\u0000",
        ]
        obj = {k: 1 for k in reversed(keys)}
        canonical = jcs_serialize(obj)
        sorted_keys = sorted(keys, key=lambda k: k.encode("utf-16-be"))
        assert list(json.loads(canonical).keys()) == sorted_keys

    def test_empty_key_and_special_characters(self):
        """Empty string key, whitespace keys, control characters in keys."""
        obj = {
            "": "empty",
            " ": "space",
            "\t": "tab",
            "\n": "newline",
            "\u0000": "null_byte",
            "a": "letter_a",
            "1": "digit_one",
        }
        canonical = jcs_serialize(obj)
        sorted_keys = sorted(obj.keys(), key=lambda k: k.encode("utf-16-be"))
        assert list(json.loads(canonical).keys()) == sorted_keys


class TestAdversarialFloatFormatting:
    """Adversarially challenge IEEE 754 float formatting against ECMAScript 6 specification and Node.js."""

    def test_signed_zeros(self):
        """RFC 8785 §3.2.2.3: -0.0 and +0.0 MUST both serialize as '0'."""
        assert es6_format_float(0.0) == "0"
        assert es6_format_float(-0.0) == "0"
        assert jcs_serialize({"a": -0.0, "b": 0.0}) == '{"a":0,"b":0}'

    def test_exact_integer_representations(self):
        """Floats with integer values within |x| < 10^21 must strip trailing .0."""
        cases = [
            (1.0, "1"),
            (-1.0, "-1"),
            (0.0, "0"),
            (42.0, "42"),
            (-42.0, "-42"),
            (100.0, "100"),
            (-100.0, "-100"),
            (1000000.0, "1000000"),
            (9007199254740991.0, "9007199254740991"),      # 2^53 - 1
            (-9007199254740991.0, "-9007199254740991"),
            (9007199254740992.0, "9007199254740992"),        # 2^53
            (1e15, "1000000000000000"),
            (1e20, "100000000000000000000"),                # 21 digits: exactly 10^20
            (-1e20, "-100000000000000000000"),
        ]
        for val, expected in cases:
            assert es6_format_float(val) == expected

    def test_exponential_boundary_1e20_vs_1e21(self):
        """ECMA-262: Numbers >= 10^21 MUST use exponential notation; 10^20 is decimal."""
        # 10^20: 21 digits decimal
        assert es6_format_float(1e20) == "100000000000000000000"
        assert es6_format_float(-1e20) == "-100000000000000000000"

        # 10^21: exponential notation
        assert es6_format_float(1e21) == "1e+21"
        assert es6_format_float(-1e21) == "-1e+21"

        # Near 10^21 boundaries
        val_below = 9.999999999999999e20
        node_below = get_nodejs_float_strings([val_below])[0]
        assert es6_format_float(val_below) == node_below

        val_above = 1.0000000000000002e21
        node_above = get_nodejs_float_strings([val_above])[0]
        assert es6_format_float(val_above) == node_above

    def test_exponential_boundary_1e_minus_6_vs_1e_minus_7(self):
        """ECMA-262: Numbers >= 10^-6 use decimal with leading zeros ('0.000001');
        Numbers < 10^-6 use exponential notation ('1e-7').
        """
        assert es6_format_float(1e-5) == "0.00001"
        assert es6_format_float(-1e-5) == "-0.00001"
        assert es6_format_float(1e-6) == "0.000001"
        assert es6_format_float(-1e-6) == "-0.000001"
        assert es6_format_float(1e-7) == "1e-7"
        assert es6_format_float(-1e-7) == "-1e-7"

        # Check fractional numbers near threshold
        boundary_cases = [
            0.000001,           # 1e-6 -> "0.000001"
            0.00000125,         # 1.25e-6 -> "0.00000125"
            0.000009999,        # 9.999e-6 -> "0.000009999"
            0.0000009999,       # 9.999e-7 -> "9.999e-7"
            0.0000001,          # 1e-7 -> "1e-7"
            0.00000005,         # 5e-8 -> "5e-8"
        ]
        node_strs = get_nodejs_float_strings(boundary_cases)
        for val, expected_node in zip(boundary_cases, node_strs):
            assert es6_format_float(val) == expected_node

    def test_node_parity_over_1000_random_and_extreme_floats(self):
        """Stress-test 1000 random and boundary floats across the entire 64-bit IEEE 754 range
        against Node.js ECMAScript 6 implementation.
        """
        test_set = [
            0.0, -0.0, 1.0, -1.0, 0.5, -0.5, 0.25, -0.25,
            0.1, 0.2, 0.3, 0.1 + 0.2,
            1e-323, 5e-324, 2.2250738585072014e-308, 1.7976931348623157e+308,
            -1.7976931348623157e+308, -5e-324,
            1e-15, 1e-10, 1e-7, 1e-6, 1e-5, 1e-4, 1e-3, 1e-2, 1e-1,
            1e1, 1e2, 1e3, 1e4, 1e5, 1e10, 1e15, 1e20, 1e21, 1e22,
            math.pi, math.e, math.sqrt(2), (1 + math.sqrt(5)) / 2,
        ]

        # Generate 1000 structured and randomized floats
        random.seed(1337)
        for _ in range(1000):
            # pick exponent spanning subnormal to max float
            exp = random.choice([
                random.uniform(-324, -308),  # subnormals
                random.uniform(-308, -20),   # very small
                random.uniform(-20, -5),     # around 1e-6 threshold
                random.uniform(-5, 5),       # near 1.0
                random.uniform(5, 20),       # around 1e21 threshold
                random.uniform(20, 308),     # very large
            ])
            sign = random.choice([1.0, -1.0])
            mant = random.uniform(1.0, 9.99999999999999)
            try:
                v = sign * mant * (10.0 ** exp)
                if not math.isnan(v) and not math.isinf(v):
                    test_set.append(v)
            except OverflowError:
                pass

        # Query Node.js in batches to prevent argv limit
        batch_size = 200
        mismatches = []
        for i in range(0, len(test_set), batch_size):
            batch = test_set[i:i + batch_size]
            node_results = get_nodejs_float_strings(batch)
            for val, node_res in zip(batch, node_results):
                py_res = es6_format_float(val)
                if py_res != node_res:
                    mismatches.append({
                        "val": val,
                        "repr": repr(val),
                        "py": py_res,
                        "node": node_res,
                    })

        assert len(mismatches) == 0, f"Found {len(mismatches)} float mismatches with Node.js: {mismatches[:5]}"

    def test_nan_and_infinity_fail_closed(self):
        """RFC 8785 forbids NaN, +Infinity, -Infinity in canonical JSON."""
        with pytest.raises(ValueError, match="RFC 8785 forbids NaN and Infinity"):
            es6_format_float(float("nan"))
        with pytest.raises(ValueError, match="RFC 8785 forbids NaN and Infinity"):
            es6_format_float(float("inf"))
        with pytest.raises(ValueError, match="RFC 8785 forbids NaN and Infinity"):
            es6_format_float(float("-inf"))

        # In jcs_serialize
        with pytest.raises(ValueError):
            jcs_serialize([1, 2, float("nan")])
        with pytest.raises(ValueError):
            jcs_serialize({"key": float("inf")})
        with pytest.raises(ValueError):
            jcs_serialize({"key": float("-inf")})


class TestAdversarialEscapingAndWhitespace:
    """Adversarially challenge string escaping and whitespace suppression."""

    def test_all_control_characters_0x00_to_0x1F(self):
        """RFC 8785 §3.2.2.2: Escaping all 32 ASCII control characters.
        Specifically:
        - 0x08 -> \\b
        - 0x09 -> \\t
        - 0x0A -> \\n
        - 0x0C -> \\f
        - 0x0D -> \\r
        - All other 0x00..0x1F -> \\u00xx with lowercase hex.
        """
        for b in range(32):
            char = chr(b)
            canonical = jcs_serialize(char)
            if b == 0x08:
                assert canonical == r'"\b"'
            elif b == 0x09:
                assert canonical == r'"\t"'
            elif b == 0x0A:
                assert canonical == r'"\n"'
            elif b == 0x0C:
                assert canonical == r'"\f"'
            elif b == 0x0D:
                assert canonical == r'"\r"'
            else:
                expected = f'"\\u{b:04x}"'
                assert canonical == expected, f"Control char 0x{b:02x} escaped improperly: {canonical}"

    def test_quotes_and_backslashes_escaped(self):
        """Quotation mark and reverse solidus MUST be escaped."""
        assert jcs_serialize('"') == r'"\""'
        assert jcs_serialize('\\') == r'"\\"'
        assert jcs_serialize('"\\"') == r'"\"\\\""'
        assert jcs_serialize(r'a\b"c/d') == r'"a\\b\"c/d"'

    def test_forward_slash_must_not_be_escaped(self):
        """RFC 8785 strictly prohibits escaping the forward slash '/'."""
        url = "https://fia.com/f1/2026/timing"
        canonical = jcs_serialize(url)
        assert canonical == f'"{url}"'
        assert r"\/" not in canonical

    def test_unicode_above_0x7F_must_not_be_escaped(self):
        """Characters >= 0x0020 (except quote and backslash) MUST NOT be escaped with \\uXXXX."""
        test_strings = [
            "é", "ñ", "ü", "Ω", "α", "β", "Москва", "东京", "🏎️", "🏁", "🏆",
            "\u007F",           # DEL
            "\u0080",           # Padding char
            "\u2028",           # Line separator (RFC 8785 says do NOT escape!)
            "\u2029",           # Paragraph separator (RFC 8785 says do NOT escape!)
        ]
        for s in test_strings:
            canonical = jcs_serialize(s)
            assert canonical == f'"{s}"'
            assert r"\u" not in canonical

    def test_zero_whitespace_in_structures(self):
        """No whitespace between object/array delimiters, colons, or commas."""
        obj = {
            "a": [1, 2, 3, {"x": True, "y": None}],
            "b": {"nested": {"empty_arr": [], "empty_obj": {}}},
        }
        canonical = jcs_serialize(obj)
        for ws in [" ", "\t", "\n", "\r"]:
            assert ws not in canonical


class TestAdversarialBankersRoundingQuantization:
    """Adversarially challenge banker's rounding (half-to-even) and float quantization."""

    def test_half_to_even_exact_ties(self):
        """Banker's rounding (round half to even) on exact half-quantum boundaries."""
        # Scale 4 (precision 0.0001)
        # Even quantum + 0.00005 -> rounds to even
        # Odd quantum + 0.00005 -> rounds to even (next number)
        assert quantize_value(1.00025, "s") == 1.0002
        assert quantize_value(1.00035, "s") == 1.0004
        assert quantize_value(1.00045, "s") == 1.0004
        assert quantize_value(1.00055, "s") == 1.0006

        # Negative ties
        assert quantize_value(-1.00025, "s") == -1.0002
        assert quantize_value(-1.00035, "s") == -1.0004

    def test_integer_rpm_quantization(self):
        """Unit 'rpm' has quantum 1.0."""
        assert quantize_value(11850.4, "rpm") == 11850.0
        assert quantize_value(11850.5, "rpm") == 11850.0   # 11850 is even
        assert quantize_value(11851.5, "rpm") == 11852.0   # 11851 is odd -> 11852
        assert quantize_value(11850.6, "rpm") == 11851.0

    def test_type_preservation(self):
        """quantize_value should preserve integers and booleans without coercion."""
        assert quantize_value(42, "s") == 42
        assert isinstance(quantize_value(42, "s"), int)
        assert quantize_value(True, "s") is True
        assert quantize_value(False, "s") is False

    def test_all_precision_matrix_units(self):
        """Verify each declared unit in PRECISION_MATRIX operates as expected."""
        units = {
            "s": (84.123456, 84.1235),
            "kph": (315.456, 315.46),
            "mps": (87.654, 87.65),
            "celsius": (28.349, 28.35),
            "bar": (1013.254, 1013.25),
            "hpa": (1013.254, 1013.25),
            "percentage": (99.987, 99.99),
            "ratio": (0.12345678, 0.123457),
            "rpm": (11850.4, 11850.0),
            "m": (123.45678, 123.4568),
        }
        for unit, (raw, expected) in units.items():
            res = quantize_value(raw, unit)
            assert res == expected, f"Unit {unit} quantization failed: {res} != {expected}"


class TestAdversarialHasherInvariants:
    """Adversarially challenge cryptographic hasher functions."""

    def test_raw_event_id_order_invariance(self):
        """Changing dictionary insertion order must NOT change raw_event_id."""
        scope1 = {"driver_id": "VER", "lap": 52, "team": "RedBull"}
        scope2 = {"team": "RedBull", "lap": 52, "driver_id": "VER"}

        st1 = {"semantics": "SESSION_ELAPSED", "unit": "s", "value": 84.1234}
        st2 = {"value": 84.1234, "unit": "s", "semantics": "SESSION_ELAPSED"}

        h1 = compute_raw_event_id("PITWALL", "Timing", "k1", "s1", scope1, st1, "sha_xxx")
        h2 = compute_raw_event_id("PITWALL", "Timing", "k1", "s1", scope2, st2, "sha_xxx")
        assert h1 == h2

    def test_raw_event_id_acquisition_bound_strictness(self):
        """CAL-DOC-007 §2.1.1: If semantics is UNKNOWN or RECEIPT_TIME_ONLY and key is empty/None,
        received_at_utc is mandatory. Otherwise ValueError.
        """
        for semantics in ["UNKNOWN", "RECEIPT_TIME_ONLY"]:
            for empty_key in [None, ""]:
                with pytest.raises(ValueError, match="received_at_utc required"):
                    compute_raw_event_id(
                        "PITWALL", "Feed", empty_key, "s1", {},
                        {"semantics": semantics, "value": None},
                        "sha", received_at_utc=None
                    )
                # Providing received_at_utc succeeds
                h = compute_raw_event_id(
                    "PITWALL", "Feed", empty_key, "s1", {},
                    {"semantics": semantics, "value": None},
                    "sha", received_at_utc="2026-11-01T00:00:00Z"
                )
                assert len(h) == 64

    def test_evidence_atom_id_list_sorting_invariance(self):
        """Passing unsorted raw_event_ids or reference_atom_ids must produce identical hash
        to sorted ones.
        """
        raw_ids_1 = ["bbbb", "aaaa", "cccc"]
        raw_ids_2 = ["aaaa", "bbbb", "cccc"]
        ref_ids_1 = ["zzzz", "yyyy"]
        ref_ids_2 = ["yyyy", "zzzz"]

        h1 = compute_evidence_atom_id(raw_ids_1, "f1", "1.0", "r1", "k1", 42.0, ref_ids_1)
        h2 = compute_evidence_atom_id(raw_ids_2, "f1", "1.0", "r1", "k1", 42.0, ref_ids_2)
        assert h1 == h2

    def test_evidence_atom_id_quantization_active(self):
        """Microscopic float noise within quantum must produce identical evidence_atom_id."""
        h1 = compute_evidence_atom_id(["raw1"], "f1", "1.0", "lap", "k1", 84.12340001, unit="s")
        h2 = compute_evidence_atom_id(["raw1"], "f1", "1.0", "lap", "k1", 84.12340002, unit="s")
        assert h1 == h2

    def test_verdict_hash_pinned_frontier_for_final(self):
        """FINAL verdict with pinned_finality_frontier overrides canonical_frontier."""
        vh1 = compute_verdict_hash(
            contract_id="c1", contract_version="1.0", contract_epoch=1,
            decision_instance_id="d1", integrity_state="HEALTHY", resolution_maturity="TUPLE_RESOLVED",
            finality_status="FINAL",
            canonical_frontier={"run_clock": "2026-11-01T01:00:00Z", "watermark": "2026-11-01T00:59:00Z"},
            corroboration_profile_by_role={}, validity_timeline=[],
            sorted_support_atom_ids=[], sorted_reference_atom_ids=[],
            pinned_finality_frontier="2026-11-01T00:55:00Z",
        )
        # Even if canonical_frontier differs, pinned_finality_frontier forces exact frontier
        vh2 = compute_verdict_hash(
            contract_id="c1", contract_version="1.0", contract_epoch=1,
            decision_instance_id="d1", integrity_state="HEALTHY", resolution_maturity="TUPLE_RESOLVED",
            finality_status="FINAL",
            canonical_frontier={"run_clock": "2026-11-01T02:00:00Z", "watermark": "2026-11-01T02:00:00Z"},
            corroboration_profile_by_role={}, validity_timeline=[],
            sorted_support_atom_ids=[], sorted_reference_atom_ids=[],
            pinned_finality_frontier="2026-11-01T00:55:00Z",
        )
        assert vh1 == vh2


class TestAdversarialJCSUnsupportedTypes:
    """Fail-closed behavior on non-JSON types."""

    def test_unsupported_types_raise_type_error(self):
        class CustomObj:
            pass

        with pytest.raises(TypeError, match="Unsupported type"):
            jcs_serialize({1, 2, 3})
        with pytest.raises(TypeError, match="Unsupported type"):
            jcs_serialize(b"raw bytes")
        with pytest.raises(TypeError, match="Unsupported type"):
            jcs_serialize(CustomObj())


class TestAdversarialClaimedUnitsAndGaps:
    """Challenge gaps between claims in worker handoff vs actual implementation."""

    def test_missing_claimed_units_in_precision_matrix(self):
        """Worker handoff claims units 'ms', 'psi', 'deg', 'pct' exist in precision matrix.
        However, if missing, quantize_value silently defaults to '0.0001' (10^-4 s).
        For 'ms' (target 0.1 ms), defaulting to 0.0001 preserves 4 decimals instead of 1.
        """
        assert "ms" in PRECISION_MATRIX, "Claimed unit 'ms' missing from PRECISION_MATRIX"
        assert "psi" in PRECISION_MATRIX, "Claimed unit 'psi' missing from PRECISION_MATRIX"
        assert "deg" in PRECISION_MATRIX, "Claimed unit 'deg' missing from PRECISION_MATRIX"
        assert "pct" in PRECISION_MATRIX, "Claimed unit 'pct' missing from PRECISION_MATRIX"

    def test_duplicate_key_collision_via_coercion(self):
        """RFC 8785 §3.2.1 forbids duplicate property keys.
        When non-string keys coerce to the same string (e.g. {1: 'a', '1': 'b'}),
        jcs_serialize must not emit duplicate keys in canonical JSON text (either reject with error or deduplicate).
        """
        data = {1: "int_key", "1": "str_key"}
        try:
            canonical = jcs_serialize(data)
            count_key = canonical.count('"1":')
            assert count_key <= 1, f"Duplicate keys emitted in canonical JSON: {canonical}"
        except ValueError:
            pass  # RFC 8785 §3.2.1 error rejection compliance

