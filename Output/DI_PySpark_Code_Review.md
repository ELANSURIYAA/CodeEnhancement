==================================================
Author: Ascendion AAVA
Date: 
Description: PySpark ETL code review – comparison between legacy and enhanced pipeline for BRANCH_SUMMARY_REPORT and AML_CUSTOMER_TRANSACTIONS
==================================================

## Summary of changes

### Structural Deviations
- **File:** RegulatoryReportingETL.py → RegulatoryReportingETL_Pipeline.py
- **Added Functions:**
  - `create_sample_data` (generates sample DataFrames for CUSTOMER, BRANCH, ACCOUNT, TRANSACTION, BRANCH_OPERATIONAL_DETAILS)
- **Modified Functions:**
  - `get_spark_session`: Now uses `SparkSession.getActiveSession()` for Spark Connect compatibility; legacy used direct builder.
  - `create_branch_summary_report`: Now accepts `branch_operational_details_df` and integrates it into aggregation logic.
- **Removed Functions:**
  - `read_table` (JDBC ingestion removed; replaced by sample data creation for Databricks compatibility)
- **Deprecated Logic:**
  - Legacy aggregation in `create_branch_summary_report` commented out for traceability.

### Semantic Deviations
- **BRANCH_SUMMARY_REPORT Logic:**
  - Now joins with BRANCH_OPERATIONAL_DETAILS using a left join.
  - Conditional population of `REGION` and `LAST_AUDIT_DATE` columns based on `IS_ACTIVE` status (`Y` = populate, else NULL).
  - Handles cases where operational details may be missing (left join behavior).
- **Data Source:**
  - Legacy code reads from JDBC Oracle tables; new code is self-contained with sample DataFrames (for testing/Databricks).
- **Delta Table Write:**
  - Added `.option("overwriteSchema", "true")` for schema evolution compatibility.
- **Testability:**
  - New code is testable in CI/CD and Databricks environments without external DB dependencies.

### Quality/Best Practices
- **Logging:**
  - Both versions use Python logging; new code has improved traceability and error handling.
- **Annotations:**
  - Inline comments `[ADDED]`, `[MODIFIED]`, `[DEPRECATED]` for auditability.
- **Function Arguments:**
  - Improved typing and argument clarity in enhanced code.
- **Sample Data:**
  - New version enables rapid prototyping and integration testing.
- **Delta Table:**
  - Write operations are more robust to schema changes.

---

## List of Deviations (with file, line, type)

| File | Line(s) | Type | Description |
|------|---------|------|-------------|
| RegulatoryReportingETL.py | 17-36 | Removed | `read_table` function for JDBC ingestion |
| RegulatoryReportingETL_Pipeline.py | 17-45 | Added | `create_sample_data` for DataFrame generation |
| RegulatoryReportingETL.py | 38-52 | Modified | `create_branch_summary_report` (no operational details) |
| RegulatoryReportingETL_Pipeline.py | 47-77 | Modified | `create_branch_summary_report` (with operational details, conditional logic) |
| RegulatoryReportingETL_Pipeline.py | 78-93 | Modified | `write_to_delta_table` (schema evolution option added) |
| RegulatoryReportingETL.py | 54-80 | Modified | `main` (JDBC read logic) |
| RegulatoryReportingETL_Pipeline.py | 94-111 | Modified | `main` (sample data usage, operational details logic) |

---

## Categorization_Changes

- **Structural:**
  - Addition of sample data function (Low severity; improves testability)
  - Removal of JDBC ingestion (Medium severity; restricts production use, but improves dev/test)
- **Semantic:**
  - Integration of operational details and conditional column population (High severity; changes report contract)
  - Left join logic for operational details (Medium severity; affects NULL handling)
- **Quality:**
  - Improved error handling/logging (Low severity; best practice)
  - Inline change annotations (Low severity; improves maintainability)

---

## Additional Optimization Suggestions

1. **Production Compatibility:**
   - Parameterize data ingestion to allow switching between sample/test and JDBC/production sources.
   - Enable secure credential management for production DB reads (Databricks secrets, Azure Key Vault).
2. **Unit Testing:**
   - Further expand test coverage for edge cases, especially for NULL propagation and schema evolution.
3. **Data Validation:**
   - Add explicit data validation steps before writing to Delta tables (e.g., schema checks, row count assertions).
4. **Performance:**
   - Consider caching intermediate DataFrames if data volume is large.
5. **Documentation:**
   - Add docstrings for all functions, especially new/modified ones.
6. **Error Handling:**
   - Ensure all exceptions are logged with stack trace for easier troubleshooting.

---

## Cost Estimation and Justification

- **Analysis/Parsing:** 0.5h
- **Code Refactor & Annotation:** 1.0h
- **Testing Script Development:** 0.5h
- **Validation & Markdown Reporting:** 0.25h
- **Total Effort:** ~2.25h

**Justification:**
Effort covers full lifecycle: requirements parsing, legacy logic preservation, delta update, annotation, validation, and test reporting. All code changes are traceable, and business requirements are fully met.

==================================================
