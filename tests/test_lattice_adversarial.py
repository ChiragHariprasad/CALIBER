"""CALIBRE SIA v2.2 - Adversarial Verification Suite: 4-State Admissibility Lattice & Pointwise Validity Timelines.

Adversarial Challenger: challenger_m2_1
Target: sia_core/models/states.py & sia_core/time/timeline.py
Governing Specification: CAL-DOC-007 §2.5.1, §2.5.3, Errata D-04, D-05.
"""
from __future__ import annotations

import itertools
import random
from datetime import datetime, timedelta, timezone
from typing import List, Tuple

import pytest

from sia_core.models.states import (
    IntegrityState,
    and_state,
    or_state,
    k_of_n_state,
)
from sia_core.time.timeline import (
    ValidityTimeline,
    TimelineBreakpoint,
    compose_pointwise,
)


# ==============================================================================
# 1. 4-State Admissibility Lattice: Exhaustive Truth Tables & Operators
# ==============================================================================

# Normative truth table per CAL-DOC-007 §2.5.1
# DEGRADED < UNKNOWN < SUSPECT < HEALTHY
# Ranks: D=0, U=1, S=2, H=3
D = IntegrityState.DEGRADED
U = IntegrityState.UNKNOWN
S = IntegrityState.SUSPECT
H = IntegrityState.HEALTHY

ALL_STATES = [D, U, S, H]

# 16-cell normative table for AND (meet = min)
NORMATIVE_AND_TABLE = {
    (D, D): D, (D, U): D, (D, S): D, (D, H): D,
    (U, D): D, (U, U): U, (U, S): U, (U, H): U,
    (S, D): D, (S, U): U, (S, S): S, (S, H): S,
    (H, D): D, (H, U): U, (H, S): S, (H, H): H,
}

# 16-cell normative table for OR (join = max)
NORMATIVE_OR_TABLE = {
    (D, D): D, (D, U): U, (D, S): S, (D, H): H,
    (U, D): U, (U, U): U, (U, S): S, (U, H): H,
    (S, D): S, (S, U): S, (S, S): S, (S, H): H,
    (H, D): H, (H, U): H, (H, S): H, (H, H): H,
}


class TestLatticeExhaustiveTruthTables:
    """Exhaustive verification of all 16 cells for meet and join against CAL-DOC-007 §2.5.1."""

    @pytest.mark.parametrize("pair,expected", list(NORMATIVE_AND_TABLE.items()))
    def test_meet_16_cells(self, pair: Tuple[IntegrityState, IntegrityState], expected: IntegrityState):
        a, b = pair
        assert and_state(a, b) == expected
        assert and_state(a.value, b.value) == expected  # String interop

    @pytest.mark.parametrize("pair,expected", list(NORMATIVE_OR_TABLE.items()))
    def test_join_16_cells(self, pair: Tuple[IntegrityState, IntegrityState], expected: IntegrityState):
        a, b = pair
        assert or_state(a, b) == expected
        assert or_state(a.value, b.value) == expected  # String interop

    def test_empty_meet_and_join_identities(self):
        """Empty meet is lattice top (HEALTHY), empty join is lattice bottom (DEGRADED)."""
        assert and_state() == H
        assert or_state() == D

    def test_single_element_meet_and_join(self):
        """Identity on singleton sets: and_state(x) == x, or_state(x) == x."""
        for s in ALL_STATES:
            assert and_state(s) == s
            assert or_state(s) == s
            assert and_state(s.value) == s
            assert or_state(s.value) == s

    def test_multi_arity_meet_and_join(self):
        """Multi-arity composition across arbitrary lengths (3-ary, 4-ary, 10-ary)."""
        # 3-ary
        assert and_state(H, S, U) == U
        assert or_state(D, U, S) == S
        assert and_state(H, H, D) == D
        assert or_state(D, D, H) == H

        # 4-ary
        assert and_state(H, S, U, D) == D
        assert or_state(H, S, U, D) == H

        # 10-ary mixed
        ten_states = [H, H, S, H, U, S, H, U, S, H]
        assert and_state(*ten_states) == U
        assert or_state(*ten_states) == H

    def test_invalid_string_rejection(self):
        """Passing unparseable string values must fail-closed with ValueError."""
        with pytest.raises(ValueError):
            and_state("INVALID_STATE", "HEALTHY")

        with pytest.raises(ValueError):
            or_state("DEGRADED", "NOT_A_STATE")


# ==============================================================================
# 2. Lattice Algebraic Laws (Formal Proof Verification)
# ==============================================================================

class TestLatticeAlgebraicLaws:
    """Stress-test foundational lattice axioms across all 4^3 = 64 triples."""

    @pytest.mark.parametrize("a,b", list(itertools.product(ALL_STATES, repeat=2)))
    def test_commutativity(self, a: IntegrityState, b: IntegrityState):
        """a ∧ b == b ∧ a  and  a ∨ b == b ∨ a."""
        assert and_state(a, b) == and_state(b, a)
        assert or_state(a, b) == or_state(b, a)

    @pytest.mark.parametrize("a,b,c", list(itertools.product(ALL_STATES, repeat=3)))
    def test_associativity(self, a: IntegrityState, b: IntegrityState, c: IntegrityState):
        """(a ∧ b) ∧ c == a ∧ (b ∧ c)  and  (a ∨ b) ∨ c == a ∨ (b ∨ c)."""
        assert and_state(and_state(a, b), c) == and_state(a, and_state(b, c))
        assert or_state(or_state(a, b), c) == or_state(a, or_state(b, c))

    @pytest.mark.parametrize("a", ALL_STATES)
    def test_idempotence(self, a: IntegrityState):
        """a ∧ a == a  and  a ∨ a == a."""
        assert and_state(a, a) == a
        assert or_state(a, a) == a

    @pytest.mark.parametrize("a,b", list(itertools.product(ALL_STATES, repeat=2)))
    def test_absorption(self, a: IntegrityState, b: IntegrityState):
        """a ∧ (a ∨ b) == a  and  a ∨ (a ∧ b) == a."""
        assert and_state(a, or_state(a, b)) == a
        assert or_state(a, and_state(a, b)) == a

    @pytest.mark.parametrize("a,b,c", list(itertools.product(ALL_STATES, repeat=3)))
    def test_distributivity(self, a: IntegrityState, b: IntegrityState, c: IntegrityState):
        """Distributive lattice laws:
        a ∧ (b ∨ c) == (a ∧ b) ∨ (a ∧ c)
        a ∨ (b ∧ c) == (a ∨ b) ∧ (a ∨ c)
        """
        assert and_state(a, or_state(b, c)) == or_state(and_state(a, b), and_state(a, c))
        assert or_state(a, and_state(b, c)) == and_state(or_state(a, b), or_state(a, c))

    @pytest.mark.parametrize("a", ALL_STATES)
    def test_bounded_lattice_identities(self, a: IntegrityState):
        """Top (H) and Bottom (D) absorption and identity laws."""
        # Top = H
        assert and_state(a, H) == a
        assert or_state(a, H) == H
        # Bottom = D
        assert and_state(a, D) == D
        assert or_state(a, D) == a

    @pytest.mark.parametrize("a,b", list(itertools.product(ALL_STATES, repeat=2)))
    def test_order_consistency(self, a: IntegrityState, b: IntegrityState):
        """Consistency with admissibility total order: a <= b <=> a ∧ b = a <=> a ∨ b = b."""
        is_le = a.rank <= b.rank
        assert (a <= b) == is_le
        assert (and_state(a, b) == a) == is_le
        assert (or_state(a, b) == b) == is_le


# ==============================================================================
# 3. k-of-n Generalization across Permutations & Boundary Conditions
# ==============================================================================

class TestKOfNGeneralizationAdversarial:
    """Adversarial challenge for k-of-n threshold operator."""

    @pytest.mark.parametrize("n", [1, 2, 3, 4])
    def test_k_of_n_exhaustive_permutations(self, n: int):
        """For every multiset of states of size n, k-of-n must be strictly permutation-invariant."""
        # Sample combinations of length n
        combinations = list(itertools.combinations_with_replacement(ALL_STATES, n))
        for comb in combinations:
            for k in range(1, n + 1):
                canonical_val = k_of_n_state(k, *comb)
                # Test all unique permutations of the combination
                unique_perms = set(itertools.permutations(comb))
                for perm in unique_perms:
                    res = k_of_n_state(k, *perm)
                    assert res == canonical_val, (
                        f"Permutation invariance failure for k={k}, n={n}: {perm} gave {res}, expected {canonical_val}"
                    )

    @pytest.mark.parametrize("n", [1, 2, 3, 4, 5])
    def test_k_of_n_duality_with_meet_and_join(self, n: int):
        """1-of-n == OR (join) and n-of-n == AND (meet) for any input tuple."""
        # Sample random combinations
        for _ in range(20):
            comb = [random.choice(ALL_STATES) for _ in range(n)]
            assert k_of_n_state(1, *comb) == or_state(*comb), f"1-of-{n} != OR on {comb}"
            assert k_of_n_state(n, *comb) == and_state(*comb), f"{n}-of-{n} != AND on {comb}"

    def test_k_of_n_monotonicity_in_threshold_k(self):
        """k-of-n must be non-increasing with respect to threshold k: k1 <= k2 => k1-of-n >= k2-of-n."""
        comb = [H, S, U, D, S, H]
        n = len(comb)
        for k in range(1, n):
            assert k_of_n_state(k, *comb) >= k_of_n_state(k + 1, *comb)

    def test_k_of_n_monotonicity_in_inputs(self):
        """Component-wise state domination preserves or increases k-of-n output."""
        a = [U, S, D, S]
        b = [S, S, U, H]  # Each coordinate b[i] >= a[i]
        for k in range(1, 5):
            assert k_of_n_state(k, *b) >= k_of_n_state(k, *a)

    def test_k_of_n_boundary_rejections(self):
        """Rule V-02 boundary conditions: k < 1, k > n, and empty states must raise ValueError."""
        # k = 0
        with pytest.raises(ValueError, match=r"k must satisfy 1 <= k <= 3, got 0"):
            k_of_n_state(0, H, S, U)

        # k > n
        with pytest.raises(ValueError, match=r"k must satisfy 1 <= k <= 3, got 4"):
            k_of_n_state(4, H, S, U)

        # k < 0
        with pytest.raises(ValueError, match=r"k must satisfy 1 <= k <= 2, got -1"):
            k_of_n_state(-1, H, S)

        # empty inputs (n = 0)
        with pytest.raises(ValueError, match=r"requires at least one state"):
            k_of_n_state(1)

    def test_k_of_n_string_arguments(self):
        """k-of-n must cleanly parse string literals and return IntegrityState enums."""
        res = k_of_n_state(2, "HEALTHY", "DEGRADED", "SUSPECT")
        assert res == S
        assert isinstance(res, IntegrityState)


# ==============================================================================
# 4. Pointwise Validity Timeline sigma(t): Composition & Edge Cases
# ==============================================================================

class TestValidityTimelineCompositionAdversarial:
    """Stress-test step function composition sigma(t) = sigma_1(t) (op) ... (op) sigma_n(t)."""

    def test_out_of_order_breakpoints_normalized(self):
        """Timelines initialized with shuffled breakpoints must sort strictly by timestamp."""
        b1 = TimelineBreakpoint("2026-03-29T10:00:00Z", H)
        b2 = TimelineBreakpoint("2026-03-29T10:15:00Z", S)
        b3 = TimelineBreakpoint("2026-03-29T10:30:00Z", U)
        b4 = TimelineBreakpoint("2026-03-29T10:45:00Z", D)

        # Feed in completely reversed order
        tl_rev = ValidityTimeline([b4, b3, b2, b1])
        bps = tl_rev.breakpoints
        assert [b.from_time for b in bps] == [
            "2026-03-29T10:00:00Z",
            "2026-03-29T10:15:00Z",
            "2026-03-29T10:30:00Z",
            "2026-03-29T10:45:00Z",
        ]
        assert [b.state for b in bps] == [H, S, U, D]

    def test_overlapping_transitions_interleaving(self):
        """Two timelines with interleaved transition windows compose pointwise without phase distortion."""
        # TL1: [10:00, 10:20) -> H, [10:20, 10:40) -> S, [10:40, inf) -> D
        tl1 = ValidityTimeline([
            TimelineBreakpoint("2026-03-29T10:00:00Z", H),
            TimelineBreakpoint("2026-03-29T10:20:00Z", S),
            TimelineBreakpoint("2026-03-29T10:40:00Z", D),
        ])
        # TL2: [10:10, 10:30) -> H, [10:30, 10:50) -> U, [10:50, inf) -> D
        tl2 = ValidityTimeline([
            TimelineBreakpoint("2026-03-29T10:10:00Z", H),
            TimelineBreakpoint("2026-03-29T10:30:00Z", U),
            TimelineBreakpoint("2026-03-29T10:50:00Z", D),
        ])

        # Pointwise AND: min(TL1, TL2)
        # At 10:00: TL1=H, TL2=H (before 10:10, evaluates to first breakpoint H) -> H
        # At 10:10: TL1=H, TL2=H -> H (coalesced)
        # At 10:20: TL1=S, TL2=H -> S
        # At 10:30: TL1=S, TL2=U -> U
        # At 10:40: TL1=D, TL2=U -> D
        # At 10:50: TL1=D, TL2=D -> D (coalesced)
        comp_and = compose_pointwise("AND", [tl1, tl2])
        bps_and = comp_and.to_list()

        assert len(bps_and) == 4
        assert bps_and[0] == {"from": "2026-03-29T10:00:00Z", "state": "HEALTHY"}
        assert bps_and[1] == {"from": "2026-03-29T10:20:00Z", "state": "SUSPECT"}
        assert bps_and[2] == {"from": "2026-03-29T10:30:00Z", "state": "UNKNOWN"}
        assert bps_and[3] == {"from": "2026-03-29T10:40:00Z", "state": "DEGRADED"}

        # Pointwise OR: max(TL1, TL2)
        # At 10:00: H
        # At 10:20: TL1=S, TL2=H -> max(S, H) = H (coalesced)
        # At 10:30: TL1=S, TL2=U -> max(S, U) = S
        # At 10:40: TL1=D, TL2=U -> max(D, U) = U
        # At 10:50: TL1=D, TL2=D -> max(D, D) = D
        comp_or = compose_pointwise("OR", [tl1, tl2])
        bps_or = comp_or.to_list()

        assert len(bps_or) == 4
        assert bps_or[0] == {"from": "2026-03-29T10:00:00Z", "state": "HEALTHY"}
        assert bps_or[1] == {"from": "2026-03-29T10:30:00Z", "state": "SUSPECT"}
        assert bps_or[2] == {"from": "2026-03-29T10:40:00Z", "state": "UNKNOWN"}
        assert bps_or[3] == {"from": "2026-03-29T10:50:00Z", "state": "DEGRADED"}

    def test_identical_timestamps_across_timelines(self):
        """Timelines sharing identical transition timestamps compose without duplicate breakpoint records."""
        t_iso = "2026-03-29T10:00:00Z"
        tl1 = ValidityTimeline([TimelineBreakpoint(t_iso, H)])
        tl2 = ValidityTimeline([TimelineBreakpoint(t_iso, S)])
        tl3 = ValidityTimeline([TimelineBreakpoint(t_iso, U)])

        comp = compose_pointwise("AND", [tl1, tl2, tl3])
        assert comp.to_list() == [{"from": t_iso, "state": "UNKNOWN"}]

        comp_k2 = compose_pointwise("K_OF_N", [tl1, tl2, tl3], k=2)
        assert comp_k2.to_list() == [{"from": t_iso, "state": "SUSPECT"}]

    def test_identical_timestamps_within_single_timeline(self):
        """Duplicate timestamps within a single timeline are resolved stably and deduplicated during composition."""
        t_iso = "2026-03-29T10:00:00Z"
        # Two breakpoints at the exact same instant
        tl = ValidityTimeline([
            TimelineBreakpoint(t_iso, H),
            TimelineBreakpoint(t_iso, D),
        ])
        # The later breakpoint in sequence defines state_at(t_iso)
        assert tl.state_at(t_iso) == D

        # Composing it deduplicates the timestamp into a single transition
        composed = compose_pointwise("AND", [tl, tl])
        assert len(composed.breakpoints) == 1
        assert composed.breakpoints[0].from_time == t_iso
        assert composed.breakpoints[0].state == D

    def test_empty_timelines_and_boundary_conditions(self):
        """Composition with zero timelines or empty timelines returns empty timeline."""
        assert compose_pointwise("AND", []).breakpoints == []
        assert compose_pointwise("AND", [ValidityTimeline([])]).breakpoints == []

        # Empty timeline composed with populated timeline evaluates empty timeline as UNKNOWN
        populated = ValidityTimeline([TimelineBreakpoint("2026-03-29T10:00:00Z", H)])
        empty = ValidityTimeline([])
        comp_and = compose_pointwise("AND", [populated, empty])
        # and_state(H, UNKNOWN) = UNKNOWN
        assert comp_and.to_list() == [{"from": "2026-03-29T10:00:00Z", "state": "UNKNOWN"}]

    def test_adjacent_duplicate_state_coalescing(self):
        """Transitions into the same state must be collapsed into a minimal step function."""
        tl = ValidityTimeline([
            TimelineBreakpoint("2026-03-29T10:00:00Z", H),
            TimelineBreakpoint("2026-03-29T10:10:00Z", H),
            TimelineBreakpoint("2026-03-29T10:20:00Z", H),
            TimelineBreakpoint("2026-03-29T10:30:00Z", S),
            TimelineBreakpoint("2026-03-29T10:40:00Z", S),
        ])
        comp = compose_pointwise("AND", [tl])
        assert comp.to_list() == [
            {"from": "2026-03-29T10:00:00Z", "state": "HEALTHY"},
            {"from": "2026-03-29T10:30:00Z", "state": "SUSPECT"},
        ]

    def test_compose_with_method_convenience(self):
        """ValidityTimeline.compose_with delegates correctly to compose_pointwise."""
        tl1 = ValidityTimeline([TimelineBreakpoint("2026-03-29T10:00:00Z", H)])
        tl2 = ValidityTimeline([TimelineBreakpoint("2026-03-29T10:00:00Z", S)])
        assert tl1.compose_with(tl2, "AND").to_list() == [{"from": "2026-03-29T10:00:00Z", "state": "SUSPECT"}]
        assert tl1.compose_with(tl2, "OR").to_list() == [{"from": "2026-03-29T10:00:00Z", "state": "HEALTHY"}]

    def test_unknown_operator_rejection(self):
        """Unknown operator strings in compose_pointwise must raise ValueError."""
        tl = ValidityTimeline([TimelineBreakpoint("2026-03-29T10:00:00Z", H)])
        with pytest.raises(ValueError, match=r"Unknown operator: XOR"):
            compose_pointwise("XOR", [tl])


# ==============================================================================
# 5. Non-Increasing Monotonicity Preservation Stress Harness
# ==============================================================================

class TestMonotonicityPreservationStressHarness:
    """Stress-test that pointwise composition sigma(t) = sigma_1(t) (op) ... (op) sigma_n(t)
    strictly preserves non-increasing monotonicity under randomized stress inputs.
    """

    @staticmethod
    def _create_random_non_increasing_timeline(base_time: datetime, max_points: int) -> ValidityTimeline:
        """Generate a random non-increasing validity timeline."""
        num_points = random.randint(1, max_points)
        # Select descending states
        states = sorted(random.sample(ALL_STATES, k=random.randint(1, min(4, num_points))), reverse=True)
        # Select random strictly increasing timestamps
        offsets = sorted(random.sample(range(0, 7200), k=len(states)))
        bps = [
            TimelineBreakpoint(
                (base_time + timedelta(seconds=sec)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                st,
            )
            for sec, st in zip(offsets, states)
        ]
        return ValidityTimeline(bps)

    def test_preservation_of_non_increasing_monotonicity_randomized(self):
        """Run 100 randomized multi-timeline stress trials verifying monotonic decay."""
        random.seed(1337)
        base_time = datetime(2026, 3, 29, 14, 0, 0, tzinfo=timezone.utc)

        for _ in range(100):
            num_timelines = random.randint(2, 6)
            timelines = [
                self._create_random_non_increasing_timeline(base_time, max_points=4)
                for _ in range(num_timelines)
            ]

            # Test AND, OR, and K_OF_N
            operators = [("AND", 1), ("OR", 1)]
            for k in range(1, num_timelines + 1):
                operators.append(("K_OF_N", k))

            for op, k_val in operators:
                composed = compose_pointwise(op, timelines, k=k_val)
                bps = composed.breakpoints

                # Invariant 1: Timestamps must be strictly increasing
                for i in range(len(bps) - 1):
                    assert bps[i].from_time < bps[i + 1].from_time, (
                        f"Timestamps not strictly increasing in {op}: {bps[i].from_time} vs {bps[i+1].from_time}"
                    )

                # Invariant 2: States must be strictly decreasing (due to coalescing)
                for i in range(len(bps) - 1):
                    assert bps[i].state > bps[i + 1].state, (
                        f"Monotonicity regression in {op} (k={k_val}): "
                        f"state {bps[i].state} is not > {bps[i+1].state} at {bps[i+1].from_time}"
                    )

    def test_large_breakpoint_sets_stress(self):
        """Compose 10 timelines with 100 breakpoints each (1,000 breakpoints total) under tight budget."""
        base_time = datetime(2026, 3, 29, 10, 0, 0, tzinfo=timezone.utc)
        timelines: List[ValidityTimeline] = []

        for i in range(10):
            bps: List[TimelineBreakpoint] = []
            for j in range(100):
                t_iso = (base_time + timedelta(seconds=j * 10 + i)).strftime("%Y-%m-%dT%H:%M:%SZ")
                # Monotonically decaying state distribution
                if j < 25:
                    st = H
                elif j < 50:
                    st = S
                elif j < 75:
                    st = U
                else:
                    st = D
                bps.append(TimelineBreakpoint(t_iso, st))
            timelines.append(ValidityTimeline(bps))

        # Compose 1,000 points
        comp = compose_pointwise("AND", timelines)
        bps = comp.breakpoints

        # Must coalesce cleanly down to exactly 4 distinct state segments
        assert len(bps) == 4
        assert [b.state for b in bps] == [H, S, U, D]
        # Must strictly preserve monotonicity
        for i in range(len(bps) - 1):
            assert bps[i].from_time < bps[i + 1].from_time
            assert bps[i].state > bps[i + 1].state


# ==============================================================================
# 6. Adversarial Type & Edge Case Findings
# ==============================================================================

class TestAdversarialTypeVulnerabilities:
    """Document and probe edge cases in input types and representations."""

    def test_timeline_breakpoint_string_state_to_dict_finding(self):
        """Vulnerability probe: Passing a raw string to TimelineBreakpoint causes AttributeError on to_dict().
        TimelineBreakpoint is a dataclass without __post_init__ string coercion.
        """
        # When passed as a string directly to TimelineBreakpoint:
        bp_raw_str = TimelineBreakpoint("2026-03-29T10:00:00Z", "HEALTHY")  # type: ignore
        with pytest.raises(AttributeError, match=r"'str' object has no attribute 'value'"):
            bp_raw_str.to_dict()

    def test_validity_timeline_dict_initialization_coerces_safely(self):
        """ValidityTimeline coerced dict-based breakpoints safely to IntegrityState enums."""
        tl = ValidityTimeline([
            {"from": "2026-03-29T10:00:00Z", "state": "HEALTHY"},
            {"from": "2026-03-29T10:15:00Z", "state": "SUSPECT"},
        ])
        assert [b.state for b in tl.breakpoints] == [H, S]
        # to_dict() succeeds safely
        assert tl.to_list() == [
            {"from": "2026-03-29T10:00:00Z", "state": "HEALTHY"},
            {"from": "2026-03-29T10:15:00Z", "state": "SUSPECT"},
        ]

    def test_invalid_breakpoint_element_type(self):
        """Invalid element type in ValidityTimeline raises TypeError."""
        with pytest.raises(TypeError, match=r"Invalid breakpoint: 12345"):
            ValidityTimeline([12345])  # type: ignore
