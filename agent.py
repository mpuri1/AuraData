import os
import json
import pandas as pd
from typing import TypedDict, Annotated, List, Dict, Any, Optional
from langgraph.graph import StateGraph, END
from langchain_openai import ChatOpenAI
from langchain.prompts import PromptTemplate
from baseline import ClaimSchema
from pydantic import ValidationError
from dotenv import load_dotenv

# Load variables from .env if present
load_dotenv()

# Define the state for the LangGraph execution
class AgentState(TypedDict):
    input_data: Dict[str, Any]      # The raw failed row dict
    errors: List[str]               # The Validation errors
    analysis_result: str            # Output of Analyzer Node
    generated_code: str             # Python code from Coder Node
    execution_success: bool         # Result of Executor
    execution_error: Optional[str]  # Stack trace if failed
    retry_count: int                # Circuit breaker counter
    fixed_data: Optional[Dict[str, Any]] # The corrected row

# Try to load standard models.
# Note: For strict adherence to the project scope, swap ChatOpenAI to ChatBedrock:
# from langchain_aws import ChatBedrock
# llm = ChatBedrock(model_id="anthropic.claude-3-sonnet-20240229-v1:0", region_name="us-east-1")
llm = ChatOpenAI(model="gpt-5.4-nano", temperature=0)

def analyzer_node(state: AgentState):
    """Analyzes the failure and root cause."""
    prompt = PromptTemplate.from_template(
        "You are an expert Data Engineer. A row of data failed Pydantic validation.\n"
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
    
    # If there was a previous error, provide it
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
        # Execute the string as Python code
        exec(code, {}, local_env)
        
        # the code must define fix_data
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
    except ValidationError as e:
        error_msgs = [f"{err['loc'][0]}: {err['msg']}" for err in e.errors()]
        return {
            "execution_success": False,
            "execution_error": f"Validation Error on output: {error_msgs}",
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
    if state["retry_count"] >= 3:
        return "max_retries"
    return "retry"

def build_graph():
    workflow = StateGraph(AgentState)
    
    workflow.add_node("analyzer", analyzer_node)
    workflow.add_node("coder", coder_node)
    workflow.add_node("executor", executor_node)
    
    workflow.set_entry_point("analyzer")
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
    if not os.environ.get("OPENAI_API_KEY"):
        print("Please set OPENAI_API_KEY environment variable. Exiting.")
        exit(1)
        
    # Read failed rows
    with open("failed_rows.json", "r") as f:
        failed_rows = json.load(f)
        
    print(f"Loaded {len(failed_rows)} failed rows. Processing first failure for demonstration...")
    test_row = failed_rows[0]
    
    initial_state = {
        "input_data": test_row["original_data"],
        "errors": test_row["errors"],
        "retry_count": 0
    }
    
    app = build_graph()
    result = app.invoke(initial_state)
    
    print("\n--- Final Agent Execution Summary ---")
    print(f"Original: {result['input_data']}")
    print(f"Success:  {result.get('execution_success', False)}")
    if result.get('execution_success'):
        print(f"Fixed:    {result['fixed_data']}")
    else:
        print(f"Final Error: {result.get('execution_error')}")
