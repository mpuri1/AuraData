import json
import os
from agent import build_graph
from dotenv import load_dotenv

load_dotenv()

def run_privacy_experiment():
    """
    Runs an A/B test between MASKING and SYNTHETIC privacy treatments.
    Demonstrates data utility vs. privacy trade-offs.
    """
    print("\n🔐 Starting AuraData Privacy Experiment...")
    
    if not os.path.exists("failed_rows.json"):
        print("⚠️ No input data found (failed_rows.json). Run baseline.py first.")
        return

    with open("failed_rows.json", "r") as f:
        failed_rows = json.load(f)

    # We'll just test on the first 3 rows for a quick experiment
    test_data = failed_rows[:3]
    
    graph = build_graph()
    results = {"MASKING": [], "SYNTHETIC": []}

    for variant in ["MASKING", "SYNTHETIC"]:
        print(f"\n--- Running Experiment Variant: {variant} ---")
        for row in test_data:
            print(f"Processing Claim: {row['original_data'].get('claim_id')}...")
            
            state = {
                "input_data": row["original_data"],
                "errors": row["errors"],
                "categories": row["categories"],
                "retry_count": 0,
                "experiment_variant": variant
            }
            
            final_state = graph.invoke(state)
            
            results[variant].append({
                "original": row["original_data"],
                "anonymized": final_state.get("anonymized_data")
            })

    # Save Results for Comparison
    with open("privacy_experiment_results.json", "w") as f:
        json.dump(results, f, indent=2)

    print("\n✅ Privacy Experiment Complete.")
    print("Compare the outputs in 'privacy_experiment_results.json'.")
    print("Variant A (MASKING) redacts; Variant B (SYNTHETIC) replaces with valid data.")

if __name__ == "__main__":
    run_privacy_experiment()
