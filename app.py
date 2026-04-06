import streamlit as st
import pandas as pd
import json
import time
from observability import ObservabilityLogger
from agent import build_graph
from dotenv import load_dotenv

# Load variables from .env if present
load_dotenv()

st.set_page_config(page_title="AuraData | Autonomous ETL", layout="wide")
st.title("AuraData: Autonomous Data Refinement Engine")

st.markdown("""
This dashboard monitors our LangGraph agent as it autonomously fixes data anomalies.
""")

if 'failed_rows' not in st.session_state:
    try:
        with open("failed_rows.json", "r") as f:
            st.session_state.failed_rows = json.load(f)
    except FileNotFoundError:
        st.session_state.failed_rows = []

if 'fixed_data' not in st.session_state:
    st.session_state.fixed_data = []

col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Total Failed Rows", len(st.session_state.failed_rows))
with col2:
    st.metric("Successfully Auto-Fixed", len(st.session_state.fixed_data))
with col3:
    st.metric("Estimated Cost Savings", f"${len(st.session_state.fixed_data) * 5.0}")

st.subheader("Run Agent Pipeline")
if st.button("Start Autonomous Fixer"):
    app = build_graph()
    logger = ObservabilityLogger()
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for i, row in enumerate(st.session_state.failed_rows):
        status_text.text(f"Processing row {i+1}...")
        
        initial_state = {
            "input_data": row["original_data"],
            "errors": row["errors"],
            "retry_count": 0
        }
        
        start_t = time.time()
        result = app.invoke(initial_state)
        end_t = time.time()
        
        claim_id = result["input_data"].get("claim_id", f"unknown-{i}")
        logger.log_agent_execution(claim_id, start_t, end_t, result)
        
        if result.get("execution_success"):
            st.session_state.fixed_data.append(result["fixed_data"])
            
        progress_bar.progress((i + 1) / len(st.session_state.failed_rows))
        
    status_text.text("Pipeline Complete. Check the Data Governance Report.")

if len(st.session_state.fixed_data) > 0:
    st.subheader("Cleaned Data Preview")
    st.dataframe(pd.DataFrame(st.session_state.fixed_data))
