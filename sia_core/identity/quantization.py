"""Float Quantization to Declared Unit Precision per CAL-DOC-007 §2.1.3."""
from __future__ import annotations

from decimal import Decimal, ROUND_HALF_EVEN
from typing import Any, Mapping

__all__ = ["quantize_value", "PRECISION_MATRIX"]

PRECISION_MATRIX: Mapping[str, str] = {
    "s": "0.0001",           # 10^-4 s (0.1 ms)
    "ms": "0.1",             # 0.1 ms
    "kph": "0.01",           # 10^-2 kph
    "mps": "0.01",           # 10^-2 mps
    "celsius": "0.01",       # 10^-2 C
    "bar": "0.01",           # 10^-2 bar
    "psi": "0.05",           # 0.05 psi
    "hpa": "0.01",           # 10^-2 hpa
    "percentage": "0.01",    # 10^-2 %
    "pct": "0.01",           # 10^-2 % alias
    "ratio": "0.000001",     # 10^-6
    "rpm": "1",              # Integer RPM
    "deg": "0.01",           # 10^-2 deg
    "m": "0.0001",           # 10^-4 m
}


def quantize_value(val: float | int, unit: str = "s") -> float | int:
    """Quantize float measurement to declared unit precision using banker's rounding."""
    if isinstance(val, bool):
        return val
    if isinstance(val, int):
        return val
    prec = PRECISION_MATRIX.get(unit.lower(), "0.0001")
    d = Decimal(str(val)).quantize(Decimal(prec), rounding=ROUND_HALF_EVEN)
    f = float(d)
    return f
