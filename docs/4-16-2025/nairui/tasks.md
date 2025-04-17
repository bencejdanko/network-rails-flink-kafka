**Nairui Liu (Lead: PyXB Parsing Module)**

*   **Primary Objective:** Prove that the provided `TS` and `Schedule` XML messages can be parsed into usable Python objects using PyXB, independent of Flink for now.
*   **Tangible Tasks & Deliverables (End of Day 1):**
    1.  **Setup PyXB Environment:** Ensure you have `PyXB` installed locally.
    2.  **Obtain Sample Data & Bindings:** Secure sample raw XML string examples for both `TS` (Forecast v3) and `Schedule` (Schedule v3) messages. Confirm you have the correct PyXB binding files (`_for.py`, `_sch3.py`).
    3.  **Implement Standalone Parsing Functions:** Create a Python file (`rtti_parser.py`) containing two functions:
        *   `parse_ts_message(xml_string)`: Takes a raw XML string, uses PyXB (`_for.py`), and returns the parsed Python object. Include error handling for invalid XML.
        *   `parse_schedule_message(xml_string)`: Takes a raw XML string, uses PyXB (`_sch3.py`), and returns the parsed Python object. Include error handling.
    4.  **Test Parsing & Field Extraction:** Write simple test cases within the script (e.g., using `if __name__ == "__main__":`) that:
        *   Call your parsing functions with the sample XML strings.
        *   Access and print key fields from the resulting Python objects (e.g., `rid`, `tpl`, `wta`, `pta`, `et` etc., as listed in Section 4) to demonstrate successful parsing.
    5.  **Identify Dependencies:** List the precise Python libraries needed for this parsing (e.g., `PyXB`). This is critical for Flink job packaging later.
*   **Accountability:** A working Python script (`rtti_parser.py`) with functions that successfully parse sample `TS` and `Schedule` XML strings into PyXB objects, demonstrated by printing extracted key fields. Dependency list provided. Status (Success/Failure/Blockers) reported by EOD.