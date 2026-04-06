import streamlit as st
import pandas as pd
import json
import time
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

# 2. Executive Summarization
st.subheader("Data Quality Governance Report")

if st.session_state.failed_rows:
    # Compile Summary Data
    summary_list = []
    for row in st.session_state.failed_rows:
        for cat in row.get("categories", ["Unknown"]):
            summary_list.append(cat)
    
    counts = pd.Series(summary_list).value_counts()
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Claims Processed", len(st.session_state.failed_rows))
    with col2:
        st.metric("Auto-Fixed (ROI)", len(st.session_state.fixed_data))
    with col3:
        st.metric("Golden Records Resolved", counts.get("Duplication", 0))
    with col4:
        st.metric("Integrity Gains", f"{round((len(st.session_state.fixed_data) / len(st.session_state.failed_rows)) * 100, 1) if st.session_state.failed_rows else 0}%")

    # Error Category Breakdown
    st.markdown("### Failure Distribution by Category")
    st.bar_chart(counts)
else:
    st.info("No failure data detected. Run the baseline validator to populate the report.")

# 3. Agent Execution
st.subheader("Refinement Execution")
if st.button("Start Global Refinement Pipeline"):
    app = build_graph()
    logger = ObservabilityLogger()
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    # In a real system we'd process all, but for demo we take a sample to keep it fast
    sample_size = min(len(st.session_state.failed_rows), 50)
    
    it_rows = st.session_state.failed_rows[:sample_size]
    
    for i, row in enumerate(it_rows):
        status_text.text(f"Refining record {i+1} of {sample_size} (Category: {', '.join(row.get('categories', []))})")
        
        initial_state = {
            "input_data": row["original_data"],
            "errors": row["errors"],
            "categories": row.get("categories", []),
            "retry_count": 0
        }
        
        start_t = time.time()
        result = app.invoke(initial_state)
        end_t = time.time()
        
        claim_id = result["input_data"].get("claim_id", f"unknown-{i}")
        logger.log_agent_execution(claim_id, start_t, end_t, result)
        
        if result.get("execution_success"):
            st.session_state.fixed_data.append(result["fixed_data"])
            
        progress_bar.progress((i + 1) / sample_size)
    
    status_text.success("Refinement Complete.")

if len(st.session_state.fixed_data) > 0:
    st.subheader("Refined 'Golden' Records")
    st.dataframe(pd.DataFrame(st.session_state.fixed_data))
