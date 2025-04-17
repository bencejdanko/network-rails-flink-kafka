**Revised Proposal & Plan**

**1. Abstract (Revised)**

This project implements a focused real-time data processing pipeline using Apache Flink (PyFlink) and Kafka to analyze UK Network Rail RTTI Push Port data. We ingest `TS` (forecast) and `Schedule` messages, parse them using PyXB bindings, and calculate real-time train delays. The core objective is to **(1)** Cluster trains with similar delay patterns using **Locality Sensitive Hashing (LSH)**; **(2)** Compute **streaming moments** (average, variance) of delays as a representative streaming algorithm; **(3)** Provide simple **Explainable AI (XAI)** insights into LSH cluster assignments; and **(4)** Apply **Differential Privacy (DP)** when reporting aggregated cluster statistics. This demonstrates an end-to-end stream processing pipeline addressing key requirements within the limited timeframe. Objectives related to platform monitoring (Bloom Filters/FM) and generic operational message analysis (FM) from the original proposal are **dropped** due to time constraints.

**2. Motivation**

Real-time RTTI data offers operational value. This project demonstrates core stream processing techniques (LSH, Moments), responsible AI principles (XAI), and privacy preservation (DP) on this data using Flink.

**3. Core Objective**

*   **Detect, Explain, and Privately Report Delay Patterns:**
    *   Ingest real-time `TS` (forecast) and `Schedule` messages via Kafka.
    *   Parse XML using PyXB bindings within Flink.
    *   Calculate real-time delays per train (`rid`) at key timing points (`tpl`) by joining/comparing forecast (`TS`) with schedule (`Schedule`) data. *Simplification: May need stateful join or enrichment.*
    *   **Apply Streaming Algorithms:**
        *   Represent recent delay evolution for a train journey segment as a feature vector.
        *   **Apply Locality Sensitive Hashing (LSH):** Use LSH within Flink (e.g., `KeyedProcessFunction`) to group trains (`rid`) exhibiting similar delay vectors into buckets.
        *   **Compute Streaming Moments:** Calculate streaming average and variance of delays for trains within each LSH bucket using Flink's stateful aggregations.
    *   **Apply Explainable AI (XAI):** Provide simple, rule-based or prototype-based explanations for LSH bucket assignment (e.g., "Pattern: High initial delay, slight recovery - similar to historical pattern X").
    *   **Apply Privacy Technique:** Apply Differential Privacy (Laplace mechanism) to the aggregated moments (average/variance) reported per LSH bucket before outputting results.

**4. Data Sources and Parsing Strategy (Simplified)**

*   **Primary Source:** Network Rail RTTI Push Port Feed (STOMP) -> Kafka.
*   **Key Message Types:** `TS` (Forecasts v3 - `_for.py`), `Schedule` (Schedules v3 - `_sch3.py`).
*   **Focus Fields:** `rid`, `tpl`, `wta`/`wtd`/`wtp` (schedule), `pta`/`ptd`, `et`/`at` (forecast).
*   **Parsing:** Use PyXB modules within PyFlink functions (`MapFunction`, `ProcessFunction`). Assumed Kafka ingestion pipeline is functional.

**5. Methodology**

*   **System Architecture:** STOMP -> Kafka (`rtti-ts`, `rtti-schedule` topics) -> Flink (PyFlink Job) -> Output (e.g., Log files, potentially another Kafka topic).
*   **Flink Implementation (Single Job):**
    *   Source: Consume from Kafka topics for `TS` and `Schedule`.
    *   Parsing: Use `MapFunction` with PyXB to convert XML strings to Python objects.
    *   Keying: Key streams primarily by `rid` (train run identifier).
    *   Stateful Processing (`KeyedProcessFunction`):
        *   Store relevant schedule information per `rid`.
        *   Receive `TS` messages, calculate delays against stored schedule.
        *   Maintain a sliding window or fixed buffer of recent delays to form a delay vector.
        *   Compute LSH signature for the delay vector. Assign train to an LSH bucket (output or update state).
    *   Aggregation (e.g., another `KeyedProcessFunction` or Windowed Aggregation keyed by LSH bucket ID):
        *   Receive delay values associated with LSH buckets.
        *   Compute streaming moments (count, sum, sum of squares for variance) per bucket.
    *   XAI: When assigning an LSH bucket, generate a simple text explanation based on vector characteristics or bucket ID.
    *   DP & Sink: Before outputting the aggregated moments per bucket, apply DP noise (e.g., in a final `MapFunction` or `SinkFunction`). Output results (Bucket ID, Count, DP Mean Delay, DP Variance Delay, Example Explanation).

**6. Evaluation**

*   **Functionality:** Demonstrate that the pipeline runs, processes messages, assigns trains to LSH buckets, calculates moments, applies DP, and generates explanations. Log outputs.
*   **LSH Grouping:** Observe if trains with visibly similar delay patterns (based on logged raw delays) end up in the same LSH buckets.
*   **DP Impact:** Show aggregated statistics before and after DP noise application (for a fixed epsilon) to illustrate the privacy/utility trade-off conceptually.
*   **Basic Performance:** Note approximate message processing rate observed during testing.

**7. Deliverables**

*   Working PyFlink code implementing the focused objective.
*   Configuration files (Flink setup, Kafka topics).
*   Sample output logs demonstrating functionality.
*   Final Report (condensed, focusing on design, implementation of required techniques, challenges, and minimal evaluation).
*   Final Presentation Slides.

**8. Team Members and Roles (Revised for Aggressive Execution)**

*   **Objective:** Deliver functional code for each component rapidly. Assume basic conceptual understanding; focus is on implementation, integration, and testing. Daily mandatory sync-ups. Proactive blocking alerts are critical.

*   **CRITICAL PATH START (Day 1-2): Foundational Infrastructure**
    *   **Jiyoon Lee:** **Lead: Flink Execution Environment & Kafka Consumer.**
        *   **Deliverable:** Runnable PyFlink environment (local or cluster) capable of consuming messages from designated Kafka topics (`rtti-ts`, `rtti-schedule`) and logging them. Define basic PyFlink job structure (`StreamExecutionEnvironment`, sources). **This MUST work by End of Day 1.**
    *   **Nairui Liu:** **Lead: PyXB Parsing Module.**
        *   **Deliverable:** Standalone Python functions using PyXB that parse provided sample `TS` and `Schedule` XML strings into defined Python object structures. Ensure PyXB dependencies are identified for Flink packaging. Concurrently, implement these functions within PyFlink `MapFunction` or `ProcessFunction` UDFs using dummy input if Flink environment isn't ready Day 1. **Parsing functions MUST work standalone by End of Day 1, integrated into Flink by End of Day 2.**

*   **CORE LOGIC IMPLEMENTATION (Day 3-5): Delay, LSH, Moments, Basic XAI**
    *   **Bence Danko:** **Lead: Delay Calculation & LSH Implementation.**
        *   **Deliverable:** PyFlink `KeyedProcessFunction` (keyed by `rid`) that:
            *   Stores necessary `Schedule` data in Flink state.
            *   Receives `TS` messages, calculates delays based on stored schedule.
            *   Constructs a delay feature vector (e.g., list of recent delays).
            *   Implements LSH hashing on the vector, assigning the train to an LSH bucket ID. Outputs `(rid, lsh_bucket_id, delay_value)`.
        *   **Requires:** Working parsed objects from Nairui, running Flink from Jiyoon.
    *   **Jiyoon Lee:** **Lead: Streaming Moments Implementation.**
        *   **Deliverable:** PyFlink operator (e.g., `KeyedProcessFunction` or windowed aggregation keyed by `lsh_bucket_id`) that:
            *   Receives `(lsh_bucket_id, delay_value)` tuples.
            *   Maintains state (count, sum, sum_of_squares) per bucket.
            *   Calculates and outputs streaming average and variance per bucket `(lsh_bucket_id, count, avg_delay, var_delay)`.
        *   **Supports:** Bence with Flink state management nuances for delay/LSH logic.
    *   **Nairui Liu:** **Lead: Parsing Integration & Basic XAI Stub.**
        *   **Deliverable:** Ensure the PyXB parsing UDFs correctly feed data into Bence's delay/LSH processor. Implement a placeholder XAI function that takes `(rid, lsh_bucket_id, delay_vector)` and outputs a basic explanation string (e.g., "Train RID assigned to Bucket X"). Refine parsing logic based on integration needs.
    *   **POTENTIAL 3-WAY PAIRING SESSION (Day 4/5):** If integration of Parsing -> Delay Calc -> LSH Hashing -> Moments Input proves difficult (state issues, data flow, performance bottlenecks), **Jiyoon, Bence, and Nairui will convene for a focused, multi-hour joint coding session** to resolve the core pipeline flow. The goal is *working code*, not perfection.

*   **ENHANCEMENTS & INTEGRATION (Day 6-7): DP, XAI Refinement, End-to-End Test**
    *   **Bence Danko:** **Lead: Differential Privacy Module.**
        *   **Deliverable:** Implement the Laplace mechanism function. Integrate it into the pipeline (e.g., a `MapFunction` after moments calculation) to add noise to `avg_delay` and `var_delay` based on a configurable epsilon. Output `(lsh_bucket_id, count, dp_avg_delay, dp_var_delay)`.
    *   **Jiyoon Lee:** **Finalize & Test Moments/DP Flow.**
        *   **Deliverable:** Ensure moments calculation is robust and DP is applied correctly before the final sink. Verify data flow and calculations end-to-end. Assist Nairui with comprehensive testing setup.
    *   **Nairui Liu:** **Lead: Integration Testing & XAI Refinement.**
        *   **Deliverable:** Refine the XAI output to be slightly more informative (e.g., based on simple vector characteristics). **CRITICAL:** Drive end-to-end pipeline testing using realistic (even if limited) data sequences. Implement logging at key stages. Identify and report bugs rigorously.

*   **FINAL PUSH (Day 8-9): Debug, Document, Deliver**
    *   **All:** **Intensive Debugging & Stabilization.** Focus solely on fixing bugs identified in Day 7 testing. Ensure the pipeline runs reliably for demonstration.
    *   **All:** **Generate Tangible Outputs.** Capture sample log outputs demonstrating LSH grouping, moments calculation (pre/post DP), and XAI messages.
    *   **All:** **CONCURRENT Documentation.** Write assigned report sections *simultaneously* in a shared document based on the implemented code and outputs. No delay allowed.
    *   **All:** **Prepare Presentation Materials.** Create slides showcasing the working components and results.

**9. Rigorous 9-Day Plan (Adjusted Emphasis)**

*   **Day 1 (Tue, Apr 16): ACTION: Build Flink Reader & Standalone Parser.**
    *   **Jiyoon:** DELIVER: Basic PyFlink job reading from Kafka test topic.
    *   **Nairui:** DELIVER: Python functions parsing sample XMLs using PyXB.
    *   **Bence:** DESIGN/CODE: Draft delay vector structure & LSH hashing function (Python code, may not be Flink-integrated yet). Design basic DP Laplace function.
    *   **End-of-Day Goal:** *Runnable* Flink Kafka consumer. *Working* local PyXB parsing code. *Code* for LSH/DP logic exists.

*   **Day 2 (Wed, Apr 17): ACTION: Integrate Parsing into Flink.**
    *   **Jiyoon & Nairui:** DELIVER: PyFlink job consuming from Kafka AND parsing messages using PyXB UDFs. Output parsed objects to logs. Resolve dependency issues.
    *   **Bence:** CODE: Implement core delay calculation logic structure within a `KeyedProcessFunction` stub. Refine LSH implementation details based on PyFlink capabilities.
    *   **End-of-Day Goal:** *Pipeline* reads Kafka -> parses -> logs structured objects *within Flink*.

*   **Day 3 (Thu, Apr 18): ACTION: Implement Delay Calculation & Initial LSH.**
    *   **Bence:** DELIVER: `KeyedProcessFunction` calculating delays and attempting LSH assignment (outputting `rid`, `lsh_bucket_id`, `delay`). Focus on state for schedule/forecast comparison.
    *   **Jiyoon:** CODE: Implement streaming moments aggregation logic structure (keyed by `lsh_bucket_id`). Assist Bence with Flink state.
    *   **Nairui:** DELIVER: Test data sequences. Integrate parser output smoothly into Bence's function. CODE: Basic XAI stub function.
    *   **End-of-Day Goal:** Delay calculation *working* in Flink state. LSH assignment *attempted*. Moments structure *coded*.

*   **Day 4 (Fri, Apr 19): ACTION: Integrate LSH & Moments Core.**
    *   **Bence:** INTEGRATE/DELIVER: Connect delay vector -> LSH hashing -> output `(rid, lsh_bucket_id, delay_value)`. Stabilize state management.
    *   **Jiyoon:** INTEGRATE/DELIVER: Connect LSH output to moments function. Implement stateful aggregation (count, sum, sum_sq). Output `(lsh_bucket_id, count, raw_avg, raw_var)`.
    *   **Nairui:** TEST/INTEGRATE: Ensure flow `Parse -> Delay/LSH -> Moments` works. Test with varied train scenarios. Implement basic XAI output string generation.
    *   **Potential 3-Way Session:** If integration fails, **All three meet and code together until fixed.**
    *   **End-of-Day Goal:** *Core pipeline* assigns LSH buckets, calculates raw moments per bucket. Basic XAI message generated.

*   **Day 5 (Sat, Apr 20): ACTION: Stabilize Core Logic & Build DP.**
    *   **Jiyoon:** REFINE/TEST: Finalize moments calculation logic. Ensure accuracy and state handling is correct.
    *   **Nairui:** REFINE: Improve XAI message based on bucket ID or simple vector properties.
    *   **Bence:** DELIVER: Working DP Laplace noise function (Python). Prepare for integration.
    *   **All:** Code review, targeted debugging session on core pipeline.
    *   **End-of-Day Goal:** Accurate raw moments calculated. Basic XAI provides some context. DP function *coded and tested standalone*.

*   **Day 6 (Sun, Apr 21): ACTION: Integrate DP & Full Pipeline Test.**
    *   **Bence:** INTEGRATE/DELIVER: Add DP noise application step to the pipeline before the sink.
    *   **Jiyoon:** TEST: Verify DP integration, check outputs pre/post noise.
    *   **Nairui:** LEAD TESTING: Execute end-to-end tests with diverse data. Log outputs rigorously. **Identify and document bugs.**
    *   **End-of-Day Goal:** *End-to-end pipeline* runs and produces DP-protected outputs. Initial list of bugs generated.

*   **Day 7 (Mon, Apr 22): ACTION: Intensive Debugging & Stabilization.**
    *   **All:** **Fix bugs identified by Nairui.** Focus on stability and correctness of the entire flow. Refine output formats for clarity. Document Flink job submission/configuration steps.
    *   **End-of-Day Goal:** A *demonstrably working and stable* system. Major bugs fixed. Ready for final output generation.

*   **Day 8 (Tue, Apr 23): ACTION: Generate Results & Write Report.**
    *   **All:** EXECUTE: Run the final pipeline on sample data, capture key output logs showing all features working.
    *   **All:** **WRITE REPORT CONCURRENTLY.** Use shared doc. Jiyoon: Flink Architecture/Setup. Nairui: Data/Parsing/XAI. Bence: LSH/Moments/DP. All: Intro/Results/Conclusion. Target >60% draft completion.
    *   **End-of-Day Goal:** Final outputs generated. Report draft substantially complete.

*   **Day 9 (Wed, Apr 24): ACTION: Finalize Report & Presentation.**
    *   **All:** COMPLETE/REVIEW: Finish writing, proofread, format report.
    *   **All:** CREATE: Build presentation slides summarizing work and results.
    *   **All:** PREPARE: Ensure code is clean, runnable, and ready for submission.
    *   **End-of-Day Goal:** Final report submitted. Presentation slides ready. Code package complete.

*   **Day 10 (Thu, Apr 25): BUFFER / Final Submission.**
    *   **All:** Final checks. Submit all deliverables by midnight.

**Key Execution Notes:**

1.  **No Slippage Tolerated:** The first two days are critical. Delays here cascade fatally.
2.  **Deliverables, Not Effort:** Focus is on producing the *working code* specified for each phase.
3.  **Radical Candor & Speed:** Communicate blockers *instantly*. Use the 3-way pairing session ruthlessly if needed on Days 4/5 to force integration.
4.  **Minimal Viable Feature:** Implement the simplest version of each component that meets the core objective. No enhancements beyond the plan.
5.  **Concurrent Documentation is Mandatory:** Reporting cannot wait until Day 9. Write as you build/test.