import pandas as pd
import random
import uuid
import numpy as np
from datetime import datetime, timedelta

def generate_mock_data(num_rows=100000):
    states = ["CA", "NY", "TX", "FL", "IL"]
    policies = ["Home", "Auto", "Life", "VIP"]
    
    data = []
    
    # Track some claim_ids to create intentional duplicates
    duplicate_candidates = []
    
    for i in range(num_rows):
        # 1. Standard Anomaly (20% logic)
        anomaly = random.random() < 0.20
        # 2. Duplicate Anomaly (Extra 5% specifically for duplicates)
        is_duplicate = random.random() < 0.05 and len(duplicate_candidates) > 0
        
        if is_duplicate:
            # Re-use an existing claim_id but provide conflicting or supplemental data
            base_record = random.choice(duplicate_candidates)
            claim_id = base_record['claim_id']
            # Conflict state or amount - Ensure we don't add to None
            state = base_record['state'] if random.random() > 0.3 else random.choice(states).lower()
            
            if isinstance(base_record['claim_amount'], (int, float)):
                claim_amount = base_record['claim_amount'] + random.randint(-100, 100) if random.random() > 0.5 else "duplicate_error"
            else:
                claim_amount = "duplicate_error"
                
            policy_type = base_record['policy_type']
            zip_code = None if random.random() < 0.3 else base_record['zip_code'] # Supplemental null
            
            try:
                date_filed = (datetime.strptime(base_record['date_filed'], "%Y-%m-%d") + timedelta(days=random.randint(1, 10))).strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                date_filed = base_record['date_filed'] # Keep the original anomaly (e.g., "yesterday")
        else:
            # Generate new record
            claim_id = str(uuid.uuid4())[:8] if not anomaly else str(uuid.uuid4())[:8] + " "
            state = random.choice(states) if not (anomaly and random.random() < 0.5) else random.choice(states).lower()
            
            # Amount anomaly
            claim_amount = round(random.uniform(500, 10000), 2)
            if anomaly and random.random() < 0.3:
                claim_amount = "five thousand" # Type anomaly
            elif anomaly and random.random() < 0.3:
                claim_amount = -500.00 # Semantic anomaly
            elif anomaly and random.random() < 0.2:
                claim_amount = None # NULL anomaly

            zip_code = str(random.randint(10000, 99999))
            if anomaly and random.random() < 0.4:
                zip_code = f"Z-{zip_code}" # format anomaly
            elif anomaly and random.random() < 0.2:
                zip_code = None # Incomplete data

            date_filed = (datetime.now() - timedelta(days=random.randint(1, 365))).strftime("%Y-%m-%d")
            if anomaly and random.random() < 0.3:
                date_filed = "yesterday"

            policy_type = random.choice(policies)

        new_row = {
            "claim_id": claim_id,
            "policy_type": policy_type,
            "state": state,
            "claim_amount": claim_amount,
            "zip_code": zip_code,
            "date_filed": date_filed
        }
        
        data.append(new_row)
        
        # Save some for duplication pool
        if not is_duplicate and len(duplicate_candidates) < 1000:
            duplicate_candidates.append(new_row)
        
    df = pd.DataFrame(data)
    df.to_csv("claims_data.csv", index=False)
    print(f"Generated {num_rows} rows of mock data (with duplicates and nulls) to claims_data.csv")

if __name__ == "__main__":
    generate_mock_data()
