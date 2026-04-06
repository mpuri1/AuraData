import pandas as pd
import random
import uuid
from datetime import datetime, timedelta

def generate_mock_data(num_rows=50):
    states = ["CA", "NY", "TX", "FL", "IL"]
    policies = ["Home", "Auto", "Life", "VIP"]
    
    data = []
    for _ in range(num_rows):
        # Introduce intentional anomalies dynamically (approx 20% of the time)
        anomaly = random.random() < 0.20
        
        claim_id = str(uuid.uuid4())[:8] if not anomaly else str(uuid.uuid4())[:8] + " "  # trailing space
        
        state = random.choice(states) if not anomaly else random.choice(states).lower()  # lowercase state
        
        # Claim amount anomaly: usually 500-10,000. Anomaly -> negative or extreme string
        claim_amount = round(random.uniform(500, 10000), 2)
        if anomaly and random.random() < 0.5:
            claim_amount = "five thousand" # Type anomaly
        elif anomaly:
            claim_amount = -500.00 # Semantic anomaly
            
        zip_code = str(random.randint(10000, 99999))
        if anomaly and random.random() < 0.5:
            zip_code = f"Z-{zip_code}" # format anomaly
            
        date = (datetime.now() - timedelta(days=random.randint(1, 365))).strftime("%Y-%m-%d")
        if anomaly and random.random() < 0.3:
            date = "yesterday"
            
        data.append({
            "claim_id": claim_id,
            "policy_type": random.choice(policies),
            "state": state,
            "claim_amount": claim_amount,
            "zip_code": zip_code,
            "date_filed": date
        })
        
    df = pd.DataFrame(data)
    df.to_csv("claims_data.csv", index=False)
    print(f"Generated {num_rows} rows of mock data with intentional anomalies to claims_data.csv")

if __name__ == "__main__":
    generate_mock_data()
