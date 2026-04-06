import pandas as pd
import numpy as np
from pydantic import BaseModel, Field, ValidationError, field_validator
from datetime import datetime
import json

class ClaimSchema(BaseModel):
    claim_id: str = Field(..., min_length=8, max_length=8)
    policy_type: str
    state: str = Field(..., min_length=2, max_length=2)
    claim_amount: float
    zip_code: str = Field(..., pattern=r"^\d{5}$")
    date_filed: str
    
    @field_validator("state")
    def state_must_be_upper(cls, v):
        if v and not v.isupper():
            raise ValueError("State must be uppercase")
        return v
        
    @field_validator("claim_amount")
    def amount_must_be_positive(cls, v):
        if isinstance(v, (int, float)) and v <= 0:
            raise ValueError("Claim amount must be positive")
        return v
        
    @field_validator("date_filed")
    def check_date_format(cls, v):
        if not v or v == "nan":
            return v
        try:
            datetime.strptime(v, "%Y-%m-%d")
            return v
        except ValueError:
            raise ValueError("Incorrect data format, should be YYYY-MM-DD")

def run_baseline_etl():
    # Load data and explicitly handle NaNs as None for Pydantic
    df = pd.read_csv("claims_data.csv").replace({np.nan: None})
    
    valid_rows = []
    invalid_rows = []
    
    # Track Duplicate Claim IDs (Lead-level check)
    # We identify all claim_ids that appear more than once
    duplicate_ids = df[df.duplicated('claim_id', keep=False)]['claim_id'].unique().tolist()
    
    for index, row in df.iterrows():
        row_dict = row.to_dict()
        errors = []
        categories = []
        
        # 1. Check for Duplicates (Batch-level failure)
        if row_dict['claim_id'] in duplicate_ids:
            errors.append(f"claim_id: Global conflict - multiple records found for this ID")
            categories.append("Duplication")
            
        # 2. Pydantic validation (Row-level failure)
        try:
            valid_row = ClaimSchema(**row_dict)
            
            # If it passed Pydantic but was a duplicate, it's still "invalid" for our refinement pipeline
            if "Duplication" in categories:
                invalid_rows.append({
                    "original_data": row_dict,
                    "errors": errors,
                    "categories": categories
                })
            else:
                valid_rows.append(valid_row.model_dump())
                
        except ValidationError as e:
            # Extract basic error messages and categorize
            for err in e.errors():
                loc = err['loc'][0]
                msg = err['msg']
                errors.append(f"{loc}: {msg}")
                
                # Categorize the error
                if "type" in err['type'] or "parsing" in err['type']:
                    categories.append("Data Type")
                elif "missing" in err['type'] or "none_allowed" in err['type']:
                    categories.append("Data Sparsity")
                else:
                    categories.append("Format/Logic")
            
            invalid_rows.append({
                "original_data": row_dict,
                "errors": errors,
                "categories": list(set(categories)) # De-duplicate categories
            })
            
    print(f"--- Baseline Validation Complete ---")
    print(f"Total Processed: {len(df)}")
    print(f"Valid Rows:      {len(valid_rows)}")
    print(f"Invalid Rows:    {len(invalid_rows)}")
    
    # Save invalid rows for the agent to process
    with open("failed_rows.json", "w") as f:
        json.dump(invalid_rows, f, indent=2)
    print("Success: Generated failed_rows.json with categorized anomalies.")

if __name__ == "__main__":
    run_baseline_etl()
