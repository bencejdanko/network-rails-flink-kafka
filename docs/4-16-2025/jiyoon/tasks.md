**Jiyoon Lee (Lead: Flink Execution Environment & Kafka Consumer)**

*   **Primary Objective:** Prove that a basic PyFlink job can connect to the designated Kafka topics and read messages. This is the absolute bedrock of the pipeline.
*   **Tangible Tasks & Deliverables (End of Day 1):**
    1.  **Setup/Verify PyFlink Environment:** Ensure you have a working local or cluster environment where you can execute a PyFlink script. Document the exact steps/commands needed to run a job (e.g., `flink run ...`).
    2.  **Identify Kafka Connection Details:** Confirm the Kafka broker address(es) and the *exact* names of the `rtti-ts` and `rtti-schedule` topics you need to connect to. If using test topics for today, clearly define those (e.g., `rtti-ts-test`, `rtti-schedule-test`). Communicate these details to the team *immediately*.
    3.  **Implement Basic Kafka Source:** Write a minimal PyFlink script (`job_day1_consumer.py`) that:
        *   Creates a `StreamExecutionEnvironment`.
        *   Defines a Kafka source connector (`FlinkKafkaConsumer`) configured to read from *at least one* of the designated topics (e.g., `rtti-ts`). Start with reading messages as raw strings.
        *   Adds a simple sink that logs or prints the raw messages received from Kafka to standard output/error or a log file.
    4.  **Execute & Verify:** Run the PyFlink job. **Crucially, demonstrate that it successfully connects to Kafka and prints/logs incoming messages.** Capture a screenshot or log snippet as proof.
*   **Accountability:** A runnable PyFlink script (`job_day1_consumer.py`) that successfully consumes messages from a designated Kafka topic, demonstrated with execution logs/output. Status of Kafka connectivity (Success/Failure/Blockers) reported by EOD.