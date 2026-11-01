import sys
sys.path.insert(0, ".")
import json
import glob
from pathlib import Path

from sia_core.contracts.parser import parse_contract_dict
from sia_core.evaluation.contract_evaluator import ContractEvaluator

for fx_path in sorted(glob.glob("tests/fixtures/FX-*.json")):
    with open(fx_path, "r", encoding="utf-8") as f:
        fx = json.load(f)
    
    fx_id = fx["fixture_id"]
    contract = parse_contract_dict(fx["target_contract"])
    input_stream = fx["input_stream"]
    schedule = fx["evaluation_schedule"]
    expected_verdicts = fx["expected_verdicts"]

    print(f"\n--- Running {fx_id}: {fx['title']} ---")
    prev_verdict = None

    for i, sched in enumerate(schedule):
        after_step = sched["after_step_index"]
        t_eval = sched["t_eval"]
        clock_domain = sched.get("clock_domain", "LIVE_UTC")
        decision_id = sched["decision_instance_id"]
        exp_v = expected_verdicts[i]

        # Feed input stream up to after_step
        if input_stream and input_stream[0].get("step_index") == 1:
            current_input = [item for item in input_stream if item.get("step_index", 0) <= after_step + 1]
        else:
            current_input = [item for item in input_stream if item.get("step_index", 0) <= after_step]

        actual_v = ContractEvaluator.evaluate(
            contract=contract,
            decision_instance_id=decision_id,
            input_stream=current_input,
            t_eval=t_eval,
            clock_domain=clock_domain,
            previous_verdict=prev_verdict,
        )

        prev_verdict = actual_v

        # Compare all 9 verdict fields per CAL-DOC-007 and dispatch requirements
        keys_to_check = [
            "integrity_state",
            "resolution_maturity",
            "finality_status",
            "verdict_hash",
            "primary_binding_witness",
            "lease_until",
            "next_transition_at",
            "validity_timeline",
            "corroboration_profile_by_role",
            "revision_number",
            "diagnosis_result",
        ]
        diffs = []
        for key in keys_to_check:
            if actual_v.get(key) != exp_v.get(key):
                diffs.append(f"{key}: actual={actual_v.get(key)} != exp={exp_v.get(key)}")

        if not diffs:
            print(f"  [PASS] Eval {sched['eval_id']} matched bit-for-bit across all 9 fields! Hash: {actual_v['verdict_hash'][:12]}")
        else:
            print(f"  [FAIL] Eval {sched['eval_id']}:")
            for d in diffs:
                print(f"     - {d}")
            print(f"     Expected Hash: {exp_v['verdict_hash']}")
            print(f"     Actual Hash:   {actual_v['verdict_hash']}")
            print(f"     Exp Timeline:  {exp_v['validity_timeline']}")
            print(f"     Act Timeline:  {actual_v['validity_timeline']}")
            print(f"     Exp Profile:   {exp_v['corroboration_profile_by_role']}")
            print(f"     Act Profile:   {actual_v['corroboration_profile_by_role']}")
            assert not diffs, f"{fx_id} {sched['eval_id']} has diffs: {diffs}"
