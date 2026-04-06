import os
import json
import pandas as pd
from typing import TypedDict, Annotated, List, Dict, Any, Optional
from langgraph.graph import StateGraph, END
from langchain_openai import ChatOpenAI
from langchain_core.prompts import PromptTemplate
from baseline import ClaimSchema
from pydantic import ValidationError
from dotenv import load_dotenv

# Load variables from .env if present
load_dotenv()

# Define the state for the LangGraph execution
class AgentState(TypedDict):
    input_data: Dict[str, Any]      # The raw failed row dict
    errors: List[str]               # The Validation errors
    categories: List[str]            # Error categories (Duplication, Sparsity, etc)
    analysis_result: str            # Output of Analyzer Node
    generated_code: str             # Python code from Coder Node
    execution_success: bool         # Result of Executor
    execution_error: Optional[str]  # Stack trace if failed
    retry_count: int                # Circuit breaker counter
    fixed_data: Optional[Dict[str, Any]] # The corrected row

llm = ChatOpenAI(model="gpt-5.4-nano", temperature=0)

def deduplication_node(state: AgentState):
    """
    Lead-Level Node: Resolves duplicate claim_id conflicts using window functions.
    This node 'heals' records by merging supplemental data from duplicates.
    """
    if "Duplication" not in state.get("categories", []):
        return state # Skip if not a duplicate
    
    # In a real environment, we'd pull all records for this ID from the DB
    # For this demo, we'll simulate the 'Window Function' resolution logic
    # We fetch the duplicates from the original claims_data.csv
    df = pd.read_csv("claims_data.csv")
    claim_id = state["input_data"]["claim_id"]
    
    # Equivalent to: ROW_NUMBER() OVER(PARTITION BY claim_id ORDER BY date_filed DESC, claim_amount DESC)
    partition = df[df['claim_id'] == claim_id].copy()
    
    # Lead Strategy: Merge supplemental data (Data Healing)
    # We take the most recent record as the 'Golden' base
    partition['date_filed'] = pd.to_datetime(partition['date_filed'], errors='coerce')
    partition = partition.sort_values(by=['date_filed', 'claim_amount'], ascending=False)
    
    golden_record = partition.iloc[0].to_dict()
    
    # Heal missing values (e.g., if golden has NULL zip_code but a duplicate has it)
    for col in partition.columns:
        if pd.isna(golden_record[col]):
            # Find the first non-null value for this column among duplicates
            non_null_values = partition[col].dropna()
            if not non_null_values.empty:
                golden_record[col] = non_null_values.iloc[0]

    # Convert everything to strings/floats for JSON compatibility
    for k, v in golden_record.items():
        if pd.isna(v): golden_record[k] = None
        elif isinstance(v, pd.Timestamp): golden_record[k] = v.strftime("%Y-%m-%d")

    return {
        "input_data": golden_record,
        "analysis_result": f"Resolved duplicate conflict using window-function partition. Selected latest record and 'healed' missing fields from {len(partition)-1} duplicates."
    }

def analyzer_node(state: AgentState):
    """Analyzes the failure and root cause."""
    prompt = PromptTemplate.from_template(
        "You are an expert Data Engineer. A row of data failed validation.\n"
        "Original Data: {data}\n"
        "Validation Errors: {errors}\n\n"
        "Explain step-by-step why the validation failed and exactly what transformation is needed to fix it."
    )
    chain = prompt | llm
    response = chain.invoke({"data": json.dumps(state["input_data"]), "errors": state["errors"]})
    
    return {"analysis_result": response.content}

def coder_node(state: AgentState):
    """Writes Python code to fix the data."""
    system_prompt = (
        "You are an expert Python Developer. You write Python code to fix data issues.\n"
        "You must return ONLY a Python function named `fix_data(row)` that takes a dictionary `row` "
        "and returns a modified dictionary `row`. DO NOT return markdown blocks, just the raw code.\n\n"
        "Original Data: {data}\n"
        "Analysis: {analysis}\n"
    )
    
    if state.get("execution_error"):
        system_prompt += "\nPrevious Code Execution Error:\n{error}\nFix the code based on the error."
        
    prompt = PromptTemplate.from_template(system_prompt)
    chain = prompt | llm
    
    inputs = {
        "data": json.dumps(state["input_data"]),
        "analysis": state["analysis_result"],
        "error": state.get("execution_error", "")
    }
    response = chain.invoke(inputs)
    
    code = response.content.strip()
    if code.startswith("```python"):
        code = code[9:]
    if code.endswith("```"):
        code = code[:-3]
        
    return {"generated_code": code.strip()}

def executor_node(state: AgentState):
    """Executes the generated code and validates the output."""
    code = state["generated_code"]
    row = dict(state["input_data"]) # copy
    
    local_env = {}
    try:
        exec(code, {}, local_env)
        if "fix_data" not in local_env:
            raise ValueError("Code did not define `fix_data` function.")
            
        fixed_row = local_env["fix_data"](row)
        
        # Verify fix via Pydantic
        ClaimSchema(**fixed_row)
        
        return {
            "execution_success": True,
            "fixed_data": fixed_row,
            "execution_error": None,
            "retry_count": state.get("retry_count", 0) + 1
        }
    except Exception as e:
        return {
            "execution_success": False,
            "execution_error": str(e),
            "retry_count": state.get("retry_count", 0) + 1
        }

def route_execution(state: AgentState):
    """Determine whether to retry coding or finish."""
    if state["execution_success"]:
        return "success"
    if state.get("retry_count", 0) >= 3:
        return "max_retries"
    return "retry"

def build_graph():
    workflow = StateGraph(AgentState)
    
    workflow.add_node("deduplicator", deduplication_node) # Lead-Level Batch Node
    workflow.add_node("analyzer", analyzer_node)
    workflow.add_node("coder", coder_node)
    workflow.add_node("executor", executor_node)
    
    workflow.set_entry_point("deduplicator")
    workflow.add_edge("deduplicator", "analyzer")
    workflow.add_edge("analyzer", "coder")
    workflow.add_edge("coder", "executor")
    
    workflow.add_conditional_edges(
        "executor",
        route_execution,
        {
            "success": END,
            "retry": "coder",
            "max_retries": END
        }
    )
    
    return workflow.compile()

if __name__ == "__main__":
    # Test stub
    pass
