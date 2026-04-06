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
from datetime import datetime

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
    # Next-Level Audit Fields
    audit_findings: List[str]       # Issues found by the Auditor node
    is_audited: bool                # Whether the audit has passed

llm = ChatOpenAI(model="gpt-5.4-nano", temperature=0)

def deduplication_node(state: AgentState):
    """Lead-Level Node: Resolves duplicate conflicts using window functions."""
    if "Duplication" not in state.get("categories", []):
        return state
    
    df = pd.read_csv("claims_data.csv")
    claim_id = state["input_data"]["claim_id"]
    partition = df[df['claim_id'] == claim_id].copy()
    partition['date_filed'] = pd.to_datetime(partition['date_filed'], errors='coerce')
    partition = partition.sort_values(by=['date_filed', 'claim_amount'], ascending=False)
    
    golden_record = partition.iloc[0].to_dict()
    for col in partition.columns:
        if pd.isna(golden_record[col]):
            non_null_values = partition[col].dropna()
            if not non_null_values.empty:
                golden_record[col] = non_null_values.iloc[0]

    for k, v in golden_record.items():
        if pd.isna(v): golden_record[k] = None
        elif isinstance(v, pd.Timestamp): golden_record[k] = v.strftime("%Y-%m-%d")

    return {
        "input_data": golden_record,
        "analysis_result": f"Resolved duplicate conflict using window functions. Partition size: {len(partition)}."
    }

def analyzer_node(state: AgentState):
    """Analyzes the failure and root cause, including audit feedback."""
    context = ""
    if state.get("audit_findings"):
        context = f"\n\n🚨 PREVIOUS AUDIT REJECTION:\nThe following logical issues were found in your previous attempt:\n{state['audit_findings']}\nYou must fix these specific semantic flaws."

    prompt = PromptTemplate.from_template(
        "You are an expert Data Engineer. A row of data failed validation.\n"
        "Original Data: {data}\n"
        "Validation Errors: {errors}{context}\n\n"
        "Explain step-by-step why the validation failed and exactly what transformation is needed to fix it."
    )
    chain = prompt | llm
    response = chain.invoke({"data": json.dumps(state["input_data"]), "errors": state["errors"], "context": context})
    
    return {"analysis_result": response.content}

def coder_node(state: AgentState):
    """Writes Python code to fix the data."""
    system_prompt = (
        "You are an expert Python Developer. You write Python code to fix data issues.\n"
        "You must return ONLY a Python function named `fix_data(row)` that takes a dictionary `row`. "
        "DO NOT return markdown blocks, just the raw code.\n\n"
        "Original Data: {data}\n"
        "Analysis: {analysis}\n"
    )
    
    if state.get("execution_error"):
        system_prompt += "\nPrevious Code Execution Error:\n{error}\nFix the code based on the error."
        
    prompt = PromptTemplate.from_template(system_prompt)
    chain = prompt | llm
    inputs = {"data": json.dumps(state["input_data"]), "analysis": state["analysis_result"], "error": state.get("execution_error", "")}
    response = chain.invoke(inputs)
    
    code = response.content.strip()
    if code.startswith("```"): code = "\n".join(code.split("\n")[1:-1])
    return {"generated_code": code.strip()}

def executor_node(state: AgentState):
    """Executes the generated code and validates the output schema."""
    code = state["generated_code"]
    row = dict(state["input_data"]) 
    local_env = {}
    try:
        exec(code, {}, local_env)
        fixed_row = local_env["fix_data"](row)
        ClaimSchema(**fixed_row)
        return {"execution_success": True, "fixed_data": fixed_row, "execution_error": None, "retry_count": state.get("retry_count", 0) + 1}
    except Exception as e:
        return {"execution_success": False, "execution_error": str(e), "retry_count": state.get("retry_count", 0) + 1}

def auditor_node(state: AgentState):
    """
    Next-Level Guardrail Node: Performs independent semantic/logical verification.
    Ensures 'hallucinations' (like futures dates or logically impossible amounts) are caught.
    """
    if not state.get("execution_success") or not state.get("fixed_data"):
        return {"is_audited": False}

    prompt = PromptTemplate.from_template(
        "You are a Data Governance Auditor. You check 'fixed' data for logical hallucinations.\n"
        "Original Data: {original}\n"
        "Fixed Data: {fixed}\n\n"
        "Instructions: \n"
        "1. Policy Logic: VIP policies must have high amounts (>500).\n"
        "2. Date Logic: The filed date cannot be in the future (Today is {today}).\n"
        "3. Semantic Check: The claim amount must be realistic for the policy type.\n\n"
        "If the data is logically perfect, respond with 'AUDIT_PASSED'.\n"
        "If there are issues, list exactly what is wrong."
    )
    
    chain = prompt | llm
    today = datetime.now().strftime("%Y-%m-%d")
    response = chain.invoke({"original": json.dumps(state["input_data"]), "fixed": json.dumps(state["fixed_data"]), "today": today})
    
    findings = []
    passed = "AUDIT_PASSED" in response.content.upper()
    if not passed:
        findings.append(response.content)
        
    return {"is_audited": passed, "audit_findings": findings}

def route_execution(state: AgentState):
    if not state["execution_success"]:
        if state.get("retry_count", 0) >= 3: return "max_retries"
        return "retry_code"
    
    # NEW Routing: Add the Auditor check
    if not state.get("is_audited"):
        if state.get("retry_count", 0) >= 3: return "max_retries"
        return "retry_audit_analysis" # Send back to analyzer with audit feedback
        
    return "success"

def build_graph():
    workflow = StateGraph(AgentState)
    
    workflow.add_node("deduplicator", deduplication_node)
    workflow.add_node("analyzer", analyzer_node)
    workflow.add_node("coder", coder_node)
    workflow.add_node("executor", executor_node)
    workflow.add_node("auditor", auditor_node) # NEW Guardrail Node
    
    workflow.set_entry_point("deduplicator")
    workflow.add_edge("deduplicator", "analyzer")
    workflow.add_edge("analyzer", "coder")
    workflow.add_edge("coder", "executor")
    workflow.add_edge("executor", "auditor") # executor leads to auditor
    
    workflow.add_conditional_edges(
        "auditor",
        route_execution,
        {
            "success": END,
            "retry_code": "coder",
            "retry_audit_analysis": "analyzer",
            "max_retries": END
        }
    )
    
    return workflow.compile()

if __name__ == "__main__":
    pass
