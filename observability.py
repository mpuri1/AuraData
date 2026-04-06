import json
import time
from datetime import datetime

class ObservabilityLogger:
    def __init__(self, log_file="governance_log.md"):
        self.log_file = log_file
        # Initialize file if not exists
        with open(self.log_file, "w") as f:
            f.write("# AI Data Governance Report\n\n")
            f.write(f"Generated on: {datetime.now().isoformat()}\n\n")
            f.write("| Row ID | Status | Anomaly Detected | Fix Applied | Cost/Tokens | Duration (s) |\n")
            f.write("|---|---|---|---|---|---|\n")

    def log_agent_execution(self, claim_id, start_time, end_time, agent_state):
        duration = round(end_time - start_time, 2)
        
        status = "✅ Fixed" if agent_state.get("execution_success") else "❌ Failed"
        
        if not agent_state.get("execution_success"):
            anomaly = "Failed to fix after retries."
            fix = f"Final Error: {agent_state.get('execution_error')}"
        else:
            anomaly = agent_state.get("analysis_result", "").replace("\n", " ")[:100] + "..."
            fix_code = agent_state.get("generated_code", "").replace("\n", " ")[:100] + "..."
            fix = f"Code: `{fix_code}`"
            
        # Simplified token cost logging (could be expanded with Langchain Callbacks)
        cost_str = f"Estimated/N/A"
        
        row_log = f"| {claim_id} | {status} | {anomaly} | {fix} | {cost_str} | {duration} |\n"
        
        with open(self.log_file, "a") as f:
            f.write(row_log)
        
        # Keep a detailed JSON trace
        trace = {
            "claim_id": claim_id,
            "timestamp": datetime.now().isoformat(),
            "duration": duration,
            "state": agent_state
        }
        with open("detailed_trace.jsonl", "a") as f:
            f.write(json.dumps(trace) + "\n")

# Example integration points later
