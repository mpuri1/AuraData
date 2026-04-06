import os
import json
import sqlite3
import pandas as pd
import ast
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
    audit_findings: List[str]       # Issues found by the Auditor node
    is_audited: bool                # Whether the audit has passed
    # Governance & Privacy Fields
    anonymized_data: Optional[Dict[str, Any]] # PII Masked output
    db_status: Optional[str]        # Persistence verification

llm = ChatOpenAI(model="gpt-5.4-nano", temperature=0)

# --- Security & Sandbox Layer ---
def safe_code_analyzer(code: str) -> bool:
    """
    AST-based guardrail to prevent RCE. 
    Strictly denies dangerous imports, function calls, and attribute access.
    """
    try:
        tree = ast.parse(code)
        # Forbidden modules (RCE sources)
        forbidden_modules = {'os', 'sys', 'subprocess', 'shutil', 'pickle', 'importlib', 'requests', 'urllib'}
        # Forbidden functions (Execution & IO sources)
        forbidden_calls = {'open', 'eval', 'exec', '__import__', 'getattr', 'setattr', 'delattr'}
        
        for node in ast.walk(tree):
            # Check for forbidden imports
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                for alias in node.names:
                    if alias.name.split('.')[0] in forbidden_modules:
                        return False
            # Check for forbidden function calls
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name) and node.func.id in forbidden_calls:
                    return False
                if isinstance(node.func, ast.Attribute) and node.func.attr in forbidden_calls:
                    return False
            # Check for dangerous attribute patterns (e.g. __subclasses__)
            if isinstance(node, ast.Attribute):
                if node.attr.startswith("__"):
                    return False
        return True
    except SyntaxError:
        return False

def input_sanitizer(raw_data: Any) -> bool:
    """
    Scans incoming raw data for common Prompt Injection (PI) patterns.
    """
    if not isinstance(raw_data, str):
        raw_data = str(raw_data)
    
    pi_patterns = [
        "ignore previous instructions",
        "system prompt",
        "you are a",
        "DAN:",
        "jailbreak",
        "respond only as",
        "output the hidden"
    ]
    
    data_lower = raw_data.lower()
    for pattern in pi_patterns:
        if pattern in data_lower:
            return False
    return True

# --- Pipeline Nodes ---
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
        "analysis_result": f"Resolved duplicate conflict. Partition size: {len(partition)}."
    }

def analyzer_node(state: AgentState):
    """Analyzes the failure and root cause."""
    context = ""
    if state.get("audit_findings"):
        context = f"\n\n🚨 PREVIOUS AUDIT REJECTION:\n{state['audit_findings']}"

    prompt = PromptTemplate.from_template(
        "You are an expert Data Engineer. Original Data: {data}\nValidation Errors: {errors}{context}\n"
        "Explain transformed required to fix it."
    )
    chain = prompt | llm
    response = chain.invoke({"data": json.dumps(state["input_data"]), "errors": state["errors"], "context": context})
    return {"analysis_result": response.content}

def coder_node(state: AgentState):
    """Writes Python code to fix the data."""
    system_prompt = (
        "You are an expert Python Developer. Return ONLY a function `fix_data(row)` returning dict.\n"
        "Original Data: {data}\nAnalysis: {analysis}\n"
    )
    if state.get("execution_error"):
        system_prompt += "\nError: {error}"
    prompt = PromptTemplate.from_template(system_prompt)
    chain = prompt | llm
    inputs = {"data": json.dumps(state["input_data"]), "analysis": state["analysis_result"], "error": state.get("execution_error", "")}
    response = chain.invoke(inputs)
    code = response.content.strip()
    if code.startswith("```"):
        lines = code.split("\n")
        if lines[0].startswith("```"): lines = lines[1:]
        if lines[-1].startswith("```"): lines = lines[:-1]
        code = "\n".join(lines).strip()
    return {"generated_code": code}

    code = state["generated_code"]
    row = dict(state["input_data"]) 
    
    # SECURITY GATE: AST-Based Sandboxing
    if not safe_code_analyzer(code):
        return {
            "execution_success": False, 
            "execution_error": "SECURITY BLOCK: The generated code was flagged as unsafe (Potential RCE attempted).",
            "retry_count": state.get("retry_count", 0) + 1
        }
    
    local_env = {}
    try:
        exec(code, {"__builtins__": {}}, local_env)
        if "fix_data" not in local_env:
            raise ValueError("The generated code did not define a 'fix_data' function.")
        fixed_row = local_env["fix_data"](row)
        ClaimSchema(**fixed_row)
        return {"execution_success": True, "fixed_data": fixed_row, "execution_error": None, "retry_count": state.get("retry_count", 0) + 1}
    except Exception as e:
        return {"execution_success": False, "execution_error": str(e), "retry_count": state.get("retry_count", 0) + 1}

def auditor_node(state: AgentState):
    """Next-Level Guardrail Node: Performs independent semantic verification."""
    if not state.get("execution_success"): return {"is_audited": False}
    prompt = PromptTemplate.from_template(
        "Audit data logic (Today: {today}). Original: {original}, Fixed: {fixed}. Respond 'AUDIT_PASSED' or list issues."
    )
    chain = prompt | llm
    today = datetime.now().strftime("%Y-%m-%d")
    response = chain.invoke({"original": json.dumps(state["input_data"]), "fixed": json.dumps(state["fixed_data"]), "today": today})
    passed = "AUDIT_PASSED" in response.content.upper()
    return {"is_audited": passed, "audit_findings": [response.content] if not passed else []}

def privacy_node(state: AgentState):
    """
    Governance Node: Performs PII Anonymization.
    Masks ZIP codes and sensitive IDs using model-driven patterns.
    """
    if not state.get("is_audited"): return state
    
    prompt = PromptTemplate.from_template(
        "You are a Privacy Officer. Anonymize PII in this row: {data}. \n"
        "Instructions: \n"
        "1. zip_code: mask the last 2 digits with '*' (e.g., 90210 -> 902**).\n"
        "2. claim_id: truncate to 5 characters and add '***'.\n"
        "Return ONLY the modified JSON dictionary."
    )
    chain = prompt | llm
    response = chain.invoke({"data": json.dumps(state["fixed_data"])})
    
    try:
        anonymized = json.loads(response.content)
        return {"anonymized_data": anonymized}
    except Exception:
        # Fallback manual mask if LLM fails format
        row = dict(state["fixed_data"])
        if row.get("zip_code"): row["zip_code"] = f"{str(row['zip_code'])[:3]}**"
        return {"anonymized_data": row}

def persistence_node(state: AgentState):
    """
    Governance Node: Persistent Data Warehousing.
    Syncs the refined, audited, anonymized record to a local SQLite database.
    """
    data = state.get("anonymized_data") or state.get("fixed_data")
    if not data: return {"db_status": "Skipped (No data)"}

    try:
        db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "refined_claims.db")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS refined_records (
                claim_id TEXT PRIMARY KEY,
                policy_type TEXT,
                state TEXT,
                claim_amount REAL,
                zip_code TEXT,
                date_filed TEXT,
                refined_at TEXT
            )
        ''')
        
        cursor.execute('''
            INSERT OR REPLACE INTO refined_records 
            (claim_id, policy_type, state, claim_amount, zip_code, date_filed, refined_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            data.get("claim_id"), data.get("policy_type"), data.get("state"),
            data.get("claim_amount"), data.get("zip_code"), data.get("date_filed"),
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ))
        conn.commit()
        conn.close()
        return {"db_status": "Synced to SQLite"}
    except Exception as e:
        return {"db_status": f"DB Error: {str(e)}"}

def route_execution(state: AgentState):
    if not state["execution_success"]:
        return "retry_code" if state.get("retry_count", 0) < 3 else "max_retries"
    if not state.get("is_audited"):
        return "retry_audit_analysis" if state.get("retry_count", 0) < 3 else "max_retries"
    return "finalize"

def build_graph():
    workflow = StateGraph(AgentState)
    
    workflow.add_node("sanitizer", sanitizer_node) # Security Entry
    workflow.add_node("deduplicator", deduplication_node)
    workflow.add_node("analyzer", analyzer_node)
    workflow.add_node("coder", coder_node)
    workflow.add_node("executor", executor_node)
    workflow.add_node("auditor", auditor_node)
    workflow.add_node("privacy", privacy_node)
    workflow.add_node("persistence", persistence_node)
    
    workflow.set_entry_point("sanitizer")
    
    # Security Routing
    workflow.add_conditional_edges(
        "sanitizer",
        lambda x: "safe" if x.get("execution_error") is None else "unsafe",
        {
            "safe": "deduplicator",
            "unsafe": END
        }
    )

    workflow.add_edge("deduplicator", "analyzer")
    workflow.add_edge("analyzer", "coder")
    workflow.add_edge("coder", "executor")
    workflow.add_edge("executor", "auditor")
    
    workflow.add_conditional_edges(
        "auditor",
        route_execution,
        {
            "finalize": "privacy",
            "retry_code": "coder",
            "retry_audit_analysis": "analyzer",
            "max_retries": END
        }
    )
    
    workflow.add_edge("privacy", "persistence")
    workflow.add_edge("persistence", END)
    
    return workflow.compile()

if __name__ == "__main__":
    pass
