import streamlit as st
import pandas as pd
import json
import time
import sqlite3
from observability import ObservabilityLogger
from agent import build_graph
from dotenv import load_dotenv

# Load variables from .env if present
load_dotenv()

st.set_page_config(page_title="AuraData | Governance Refinement", layout="wide")
st.title("AuraData: Autonomous Governance & Refinement Engine")

# 1. State Initialization
if 'failed_rows' not in st.session_state:
    try:
        with open("failed_rows.json", "r") as f:
            st.session_state.failed_rows = json.load(f)
    except FileNotFoundError:
        st.session_state.failed_rows = []

if 'fixed_data' not in st.session_state:
    st.session_state.fixed_data = []

if 'anonymized_data' not in st.session_state:
    st.session_state.anonymized_data = []

if 'audit_rejections' not in st.session_state:
    st.session_state.audit_rejections = 0

# System Security & Compliance 
st.sidebar.markdown("## Governance Strategy")
st.sidebar.info("""
**Semantic Guardrails**: 
Independent LLM Auditor node performs logical consistency checks with a prioritized rejection loop for semantic anomalies.

**Privacy & Persistence**:
PII Masking (ZIP/ID) and automated SQLite synchronization for persistent 'Golden Record' warehousing.
""")

st.subheader("Data Quality Governance Report")

if st.session_state.failed_rows:
    summary_list = []
    for row in st.session_state.failed_rows:
        for cat in row.get("categories", ["Unknown"]):
            summary_list.append(cat)
    
    counts = pd.Series(summary_list).value_counts()
    
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Total Rows", len(st.session_state.failed_rows))
    with col2:
        st.metric("Refined (ROI)", len(st.session_state.fixed_data))
    with col3:
        st.metric("Audited & Secured", len(st.session_state.anonymized_data))
    with col4:
        st.metric("Hallucinations Blocked", st.session_state.audit_rejections)
    with col5:
        st.metric("Integrity Gains", f"{round((len(st.session_state.anonymized_data) / len(st.session_state.failed_rows)) * 100, 1) if st.session_state.failed_rows else 0}%")

    st.markdown("### Failure Distribution by Category")
    st.bar_chart(counts)
else:
    st.info("No failure data detected.")

# 3. Agent Execution
st.subheader("Refinement Execution")
st.markdown("> [!TIP]\n> **Execution Strategy**: The LangGraph pipeline uses **GPT-5.4 Nano** to analyze schema violations, generate corrective Python code, and audit the results for logical semantic consistency before PII masking and SQLite persistence.")

if st.button("Start Global Refinement Pipeline"):
    app = build_graph()
    logger = ObservabilityLogger()
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    sample_size = min(len(st.session_state.failed_rows), 50)
    it_rows = st.session_state.failed_rows[:sample_size]
    
    for i, row in enumerate(it_rows):
        status_text.text(f"Processing record {i+1} of {sample_size}...")
        
        initial_state = {
            "input_data": row["original_data"],
            "errors": row["errors"],
            "categories": row.get("categories", []),
            "retry_count": 0,
            "audit_findings": []
        }
        
        start_t = time.time()
        result = app.invoke(initial_state)
        end_t = time.time()
        
        if result.get("audit_findings") and not result.get("is_audited"):
            st.session_state.audit_rejections += 1

        claim_id = result["input_data"].get("claim_id", f"unknown-{i}")
        logger.log_agent_execution(claim_id, start_t, end_t, result)
        
        if result.get("execution_success") and result.get("is_audited"):
            st.session_state.fixed_data.append(result["fixed_data"])
            if result.get("anonymized_data"):
                st.session_state.anonymized_data.append(result["anonymized_data"])
            
        progress_bar.progress((i + 1) / sample_size)
    
    status_text.success("Pipeline Execution Complete.")

# 4. Refined Warehouse View
st.subheader("Refined Anonymized Warehouse (SQLite Backend)")
if len(st.session_state.anonymized_data) > 0:
    st.markdown("> [!IMPORTANT]\n> **PII Protection Active**: Claim IDs and ZIP codes have been asynchronously anonymized by the Privacy Node before persisting to SQLite.")
    
    tab1, tab2 = st.tabs(["Warehouse Preview", "Database Integrity"])
    
    with tab1:
        st.dataframe(pd.DataFrame(st.session_state.anonymized_data))
    
    with tab2:
        try:
            conn = sqlite3.connect("refined_claims.db")
            db_df = pd.read_sql_query("SELECT * FROM refined_records LIMIT 10", conn)
            st.write("Latest Persistent Records in SQLite:")
            st.dataframe(db_df)
            conn.close()
        except Exception as e:
            st.error(f"Could not connect to Warehouse: {e}")
else:
    st.info("The Warehouse is currently empty. Run the Refinement Pipeline to begin population.")
