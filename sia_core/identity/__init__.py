"""SIA Core Identity Module: RFC 8785 JCS Serialization and Canonical Identity Hashes."""
from .canonical import (
    jcs_serialize,
    jcs_serialize_bytes,
    jcs_sha256,
    es6_format_float,
    canonicalize,
)
from .quantization import quantize_value, PRECISION_MATRIX
from .hasher import (
    compute_raw_event_id,
    compute_evidence_atom_id,
    compute_verdict_hash,
)

__all__ = [
    "jcs_serialize",
    "jcs_serialize_bytes",
    "jcs_sha256",
    "es6_format_float",
    "canonicalize",
    "quantize_value",
    "PRECISION_MATRIX",
    "compute_raw_event_id",
    "compute_evidence_atom_id",
    "compute_verdict_hash",
]
