import pandas as pd
from pydantic import BaseModel, Field, ValidationError, validator
from datetime import datetime
import json

class ClaimSchema(BaseModel):
    claim_id: str = Field(..., min_length=8, max_length=8)
    policy_type: str
    state: str = Field(..., min_length=2, max_length=2)
    claim_amount: float
    zip_code: str = Field(..., pattern=r"^\d{5}$")
    date_filed: str
    
    @validator("state")
    def state_must_be_upper(cls, v):
        if not v.isupper():
            raise ValueError("State must be uppercase")
        return v
        
    @validator("claim_amount")
    def amount_must_be_positive(cls, v):
        if v <= 0:
            raise ValueError("Claim amount must be positive")
        return v
        
    @validator("date_filed")
    def check_date_format(cls, v):
        try:
            datetime.strptime(v, "%Y-%m-%d")
            return v
        except ValueError:
            raise ValueError("Incorrect data format, should be YYYY-MM-DD")

def run_baseline_etl():
    df = pd.read_csv("claims_data.csv")
    valid_rows = []
    invalid_rows = []
    
    for index, row in df.iterrows():
        try:
            # Convert row to dict
            row_dict = row.to_dict()
            # Pydantic validation
            valid_row = ClaimSchema(**row_dict)
            valid_rows.append(valid_row.dict())
        except ValidationError as e:
            # Extract basic error messages
            error_msgs = [f"{err['loc'][0]}: {err['msg']}" for err in e.errors()]
            invalid_rows.append({
                "original_data": row_dict,
                "errors": error_msgs
            })
            
    print(f"Baseline Validation Complete")
    print(f"Valid Rows: {len(valid_rows)}")
    print(f"Invalid Rows (Dropped): {len(invalid_rows)}")
    
    # Save invalid rows for the agent to process
    with open("failed_rows.json", "w") as f:
        json.dump(invalid_rows, f, indent=2)

if __name__ == "__main__":
    run_baseline_etl()
