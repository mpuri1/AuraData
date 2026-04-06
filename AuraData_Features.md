# AuraData: Technical Feature Specification

AuraData is an **Autonomous Data Governance & Refinement Engine** designed to solve high-scale data integrity challenges using Agentic AI (LangGraph) and Advanced Data Engineering patterns.

---

## 1. Core Features

### 🧠 Agentic Self-Correction (LangGraph)
Instead of hardcoded "If/Then" rules, AuraData uses a multi-node cyclical graph to resolve data anomalies:
- **Analyzer Node**: Discovers the root cause of Pydantic validation failures.
- **Coder Node**: Dynamically writes context-aware Python correction code.
- **Executor Node**: Runs the generated code in a sandboxed environment and re-validates the output.

### 🏆 Golden Record Resolution (Window Functions)
Handles data collisions at scale using senior-level partitioning logic:
- Uses **Pandas Windowing (`rank()`)** to identify the most authoritative record in a duplicate set.
- Performs **"Data Healing"**: Merges supplemental fields from secondary (discarded) records into the primary record to ensure no data loss.

### 📊 Executive Governance Dashboard
A professional Streamlit UI that provides real-time auditability:
- **Failure Distribution**: Categorizes anomalies by Duplication, Sparsity, Type, and Logic.
- **ROI Tracking**: Calculates estimated cost/time savings from autonomous refinement.
- **Cleaned Data Preview**: Instant verification of the refined dataset.

---

## 2. Handled Cases

| Category | Description | Example Case |
| :--- | :--- | :--- |
| **Schema Violations** | Standard Pydantic/Type mismatch errors. | `claim_amount` represented as a string "five thousand". |
| **Semantic Errors** | Logically impossible data points. | Negative `claim_amount` or "yesterday" as a filed date. |
| **Format Issues** | Inconsistent regional or system formatting. | Lowercase `state` codes or non-numeric `zip_code` suffixes. |
| **Data Sparsity** | Incomplete records missing critical fields. | `NULL` values in Zip Code or Policy Type. |

---

## 3. High-End Edge Cases

AuraData is uniquely architected to handle complex "Lead-level" scenarios:

> [!TIP]
> **Claim ID Collisions**
> **Problem**: Two records share the same ID but have different timestamps and amounts. 
> **Resolution**: AuraData uses a window partition to pick the latest record and merges any non-conflicting data from the clone into the master record.

> [!IMPORTANT]
> **Dynamic Code Recovery**
> **Problem**: The AI-generated correction code fails during execution.
> **Resolution**: The LangGraph state captures the stack trace and feeds it back into the **Coder Node**, enabling the agent to autonomously debug and fix its own code through up to 3 retry cycles.

---

## 4. Value Proposition (ROI)

1. **Reduced Data Latency**: Traditional manual data cleaning for 20,000 failures can take weeks; AuraData processes them in near real-time.
2. **Infinite Scalability**: Built to handle 100,000+ rows using a hybrid Batch + Agentic approach.
3. **Auditability & Compliance**: Every fix is logged in an immutable Governance Report, ensuring that "AI-manipulated" data remains transparent and traceable.
4. **Resilience**: The system "heals" itself from data sparsity, recovering value from records that would otherwise be rejected and dropped.
