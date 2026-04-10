import json
import pandas as pd
from agent import build_graph
from governance_engine import GovernanceEngine

def test_governance_suite():
    print("🚀 Initializing Lead-Level Governance Verification...")
    
    # Setup
    gov = GovernanceEngine()
    
    # Case 1: ROI Calculation
    print("\n[Test 1] ROI Accuracy:")
    roi = gov.calculate_refinement_roi(100)
    print(f"  - ROI Metrics: {roi}")
    assert roi["net_roi"] > 0
    assert roi["efficiency_multiplier"] > 10
    print("  ✅ ROI Metrics Verified.")

    # Case 2: Fairness Proxy Audit (Success)
    print("\n[Test 2] Fairness Audit (Normal Refinement):")
    original = {"claim_id": "C001", "state": "CA", "claim_amount": 1000, "zip_code": "90210"}
    refined = {"claim_id": "C001", "state": "CA", "claim_amount": 1050, "zip_code": "90210"}
    audit = gov.calculate_fairness_risk(original, refined)
    print(f"  - Risk Score: {audit['risk_score']}")
    assert audit["risk_score"] < 0.2
    print("  ✅ Fairness Baseline Verified.")

    # Case 3: Bias Injection (Geographic Proxy Shift)
    print("\n[Test 3] Bias Injection (Proxy Detection):")
    biased_refined = {"claim_id": "C001", "state": "NY", "claim_amount": 2500, "zip_code": "10001"}
    bias_audit = gov.calculate_fairness_risk(original, biased_refined)
    print(f"  - Flagged Risk Score: {bias_audit['risk_score']}")
    print(f"  - Audit Findings: {bias_audit['findings']}")
    assert bias_audit["risk_score"] > 0.5
    assert any("Geographic" in f for f in bias_audit["findings"])
    print("  ✅ Proxy Bias Guardrail Verified.")

    print("\n🏆 GOVERNANCE VERIFICATION COMPLETE: ALL SYSTEMS NOMINAL.")

if __name__ == "__main__":
    test_governance_suite()
