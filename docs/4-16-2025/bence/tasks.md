**Bence Danko (Design/Code: Delay, LSH, DP)**

*   **Primary Objective:** Design and draft the core algorithmic components (Delay Vector, LSH, DP) as standalone Python code. This prepares the logic for integration into Flink later.
*   **Tangible Tasks & Deliverables (End of Day 1):**
    1.  **Define Delay Vector Structure:** Decide and document how the "delay feature vector" will be represented. *Example:* A fixed-size list/array (e.g., size 5) containing the last 5 calculated delay values (in minutes) for a train at consecutive timing points it passed. Specify *what* constitutes a delay (e.g., `actual_time - scheduled_time`). Document this structure clearly in comments or a design note.
    2.  **Draft LSH Hashing Function:** Write a standalone Python function (`lsh_hasher.py` or similar) that:
        *   Takes a delay vector (as defined above) as input.
        *   Implements a *simple* LSH technique (e.g., random projection). Don't overcomplicate it initially. Focus on getting *a* hash bucket ID out.
        *   Returns an LSH bucket identifier (e.g., an integer or a string).
        *   *Note:* This does not need to be integrated with Flink state today.
    3.  **Draft DP Laplace Function:** Write a standalone Python function (`dp_utils.py` or similar) that:
        *   Implements the Laplace mechanism: `add_laplace_noise(value, sensitivity, epsilon)`.
        *   Takes a numerical value, a sensitivity value (Δf), and an epsilon (ε) as input.
        *   Returns the value plus appropriately scaled Laplace noise.
        *   Make initial, documented assumptions for sensitivity (e.g., based on a maximum plausible delay) and choose a placeholder epsilon (e.g., 1.0).
*   **Accountability:** Python files containing the draft code for the LSH hashing function and the DP Laplace noise function, along with a clear definition/documentation of the chosen delay vector structure. The code should exist and be conceptually sound. Status (Code Drafted/Blocked) reported by EOD.