# AuraData: Autonomous Data Refinement Engine

## Overview
This repository contains a self-correcting Agentic ETL pipeline. It replaces brittle, rules-based static data validation pipelines using an **LLM-as-a-Judge** and an autonomous **LangGraph self-correcting coding loop**.

When standard Pydantic validation fails, instead of dropping the row, the system uses an LLM to analyze the root cause of the error, dynamically generate a continuous Python correction script in pandas, execute the code against the row, and log the execution loop into a Markdown Data Governance Report.

## Architecture

This project is built using:
- **LangGraph**: Used instead of CrewAI because ETL requires tightly controlled, cyclic, and stateful DAG execution rather than pure conversational personas.
- **Pydantic**: The baseline structure validator for establishing data ground truth.
- **Streamlit & Docker**: Provides the interactive evaluation UI and containerizes the solution.

### LangGraph Nodes
1. **Analyzer**: Prompted with the original bad data point and the Pydantic error trace. Its job is to output a root cause logic (e.g., "The state column 'florida' is lowercase, but requires exactly two uppercase letters").
2. **Coder**: Ingests the Analysis and writes a strict `fix_data(row)` python function.
3. **Executor**: Sandboxed Python runner that evaluates the Coder's script against the row. Triggers a loop back to the Coder if a new python exception is raised.

## How to Run

1. Navigate to the project directory:
   ```bash
   cd AuraData
   ```
2. Make sure you have your dependencies installed:
   ```bash
   pip install -r requirements.txt
   ```
3. Set your API Key (Default is OpenAI `gpt-5.4-nano`, but it is configured for easy drop-in AWS Bedrock swapping):
   ```bash
   export OPENAI_API_KEY="sk-..."
   ```
4. Run the visual Dashboard:
   ```bash
   streamlit run app.py
   ```

## Logs and Governance

The Observability module tracks the time complexity, tokens, state outputs, and generated fixes for every pipeline invocation. Once the pipeline completes a cycle, check `governance_log.md` and `detailed_trace.jsonl` to audit exactly *why* a particular piece of financial row data was fundamentally altered.
