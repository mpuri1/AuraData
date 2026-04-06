import json
import os

import pandas as pd
import streamlit as st

st.set_page_config(page_title="AuraData", page_icon="🔄", layout="wide")

st.title("AuraData: Autonomous Data Refinement Engine")
st.caption("Self-correcting Agentic ETL pipeline powered by LangGraph and LLM-as-a-Judge")

st.info(
    "Set your API key via the `OPENAI_API_KEY` environment variable, "
    "then upload a CSV to begin validation and correction.",
    icon="ℹ️",
)

uploaded = st.file_uploader("Upload a CSV file for validation", type=["csv"])

if uploaded is not None:
    df = pd.read_csv(uploaded)
    st.subheader("Raw Data Preview")
    st.dataframe(df, use_container_width=True)

    if st.button("Run Agentic Validation & Correction"):
        try:
            from agent import run_pipeline  # type: ignore[import]

            with st.spinner("Running LangGraph correction pipeline…"):
                results = run_pipeline(df)

            st.success("Pipeline complete.")
            st.subheader("Corrected Data")
            st.dataframe(results, use_container_width=True)
        except ImportError:
            st.error(
                "Agent module not found. Ensure `agent.py` is present and dependencies are installed."
            )
        except Exception as exc:
            st.error(f"Pipeline error: {exc}")

st.divider()
col1, col2 = st.columns(2)
with col1:
    st.subheader("Governance Log")
    try:
        with open("governance_log.md") as f:
            st.markdown(f.read())
    except FileNotFoundError:
        st.caption("No governance log found yet. Run the pipeline to generate one.")

with col2:
    st.subheader("Detailed Trace")
    trace_path = "detailed_trace.jsonl"
    if os.path.exists(trace_path):
        traces = []
        with open(trace_path) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        traces.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
        if traces:
            st.json(traces[-1])
        else:
            st.caption("Trace file is empty.")
    else:
        st.caption("No trace file found yet. Run the pipeline to generate one.")
