"""RFC 8785 JSON Canonicalization Scheme (JCS) Serializer and Hasher.

Pure Python, zero-dependency implementation of RFC 8785.
Guarantees identical output to ECMAScript 6 JSON serialization and Java RFC 8785 engines.
"""
from __future__ import annotations

import hashlib
import math
from typing import Any

__all__ = [
    "jcs_serialize",
    "jcs_serialize_bytes",
    "jcs_sha256",
    "es6_format_float",
    "canonicalize",
]

BS = chr(92)       # Backslash '\'
DQUOTE = chr(34)   # Double quote '"'


def es6_format_float(val: float) -> str:
    """Format IEEE 754 double precision float per ECMAScript 6 Section 7.1.12.1 ToString(Number)."""
    if math.isnan(val) or math.isinf(val):
        raise ValueError(f"RFC 8785 forbids NaN and Infinity in JSON: {val}")
    if val == 0.0:
        return "0"

    sign = "-" if math.copysign(1.0, val) < 0 else ""
    val = abs(val)

    # Python repr() uses David Gay's dtoa, guaranteeing shortest round-tripping representation
    r = repr(val)
    if "e" in r or "E" in r:
        r_lower = r.lower()
        mantissa, exp_str = r_lower.split("e")
        exp = int(exp_str)
        if "." in mantissa:
            int_part, frac_part = mantissa.split(".")
            digits = int_part + frac_part
            n = len(int_part) + exp
        else:
            digits = mantissa
            n = len(mantissa) + exp
    else:
        if "." in r:
            int_part, frac_part = r.split(".")
            if frac_part == "0":
                digits = int_part
                n = len(int_part)
            else:
                digits = int_part + frac_part
                if int_part == "0":
                    num_leading_zeros = len(frac_part) - len(frac_part.lstrip("0"))
                    digits = frac_part.lstrip("0")
                    n = -num_leading_zeros
                else:
                    n = len(int_part)
        else:
            digits = r
            n = len(r)

    k = len(digits)

    # ECMA-262 7.1.12.1 formatting rules:
    if k <= n <= 21:
        return sign + digits + ("0" * (n - k))
    elif 0 < n <= 21:
        return sign + digits[:n] + "." + digits[n:]
    elif -6 < n <= 0:
        return sign + "0." + ("0" * (-n)) + digits
    else:
        exp_val = n - 1
        exp_sign = "+" if exp_val >= 0 else "-"
        exp_formatted = f"e{exp_sign}{abs(exp_val)}"
        if k == 1:
            return sign + digits + exp_formatted
        else:
            return sign + digits[0] + "." + digits[1:] + exp_formatted


def _jcs_serialize_string(s: str) -> str:
    """Serialize string per RFC 8785 Section 3.2.2.2.
    
    Escapes quotes, backslashes, and ASCII control characters (0x00-0x1F).
    Does NOT escape forward slashes or UTF-8 characters >= 0x0020.
    """
    res = [DQUOTE]
    for c in s:
        cp = ord(c)
        if c == DQUOTE:
            res.append(BS + DQUOTE)
        elif c == BS:
            res.append(BS + BS)
        elif c == "\b":
            res.append(BS + "b")
        elif c == "\t":
            res.append(BS + "t")
        elif c == "\n":
            res.append(BS + "n")
        elif c == "\f":
            res.append(BS + "f")
        elif c == "\r":
            res.append(BS + "r")
        elif cp < 0x20:
            res.append(f"{BS}u{cp:04x}")
        else:
            res.append(c)
    res.append(DQUOTE)
    return "".join(res)


def jcs_serialize(data: Any) -> str:
    """Serialize arbitrary Python data structure into RFC 8785 Canonical JSON text."""
    if data is None:
        return "null"
    elif isinstance(data, bool):
        return "true" if data else "false"
    elif isinstance(data, int):
        return str(data)
    elif isinstance(data, float):
        return es6_format_float(data)
    elif isinstance(data, str):
        return _jcs_serialize_string(data)
    elif isinstance(data, (list, tuple)):
        return "[" + ",".join(jcs_serialize(x) for x in data) + "]"
    elif isinstance(data, dict):
        if len(set(str(k) for k in data.keys())) != len(data):
            raise ValueError("Duplicate property key in JSON object")
        # Sort keys lexicographically by UTF-16 code units using big-endian byte encoding
        sorted_keys = sorted(data.keys(), key=lambda k: str(k).encode("utf-16-be"))
        items = [_jcs_serialize_string(str(k)) + ":" + jcs_serialize(data[k]) for k in sorted_keys]
        return "{" + ",".join(items) + "}"
    else:
        raise TypeError(f"Unsupported type for RFC 8785 canonicalization: {type(data)}")


def jcs_serialize_bytes(data: Any) -> bytes:
    """Serialize data into canonical UTF-8 encoded bytes."""
    return jcs_serialize(data).encode("utf-8")


def jcs_sha256(data: Any) -> str:
    """Compute 64-character lowercase hexadecimal SHA-256 digest of RFC 8785 canonical serialization."""
    return hashlib.sha256(jcs_serialize_bytes(data)).hexdigest()


# Alias for convenience
canonicalize = jcs_serialize
