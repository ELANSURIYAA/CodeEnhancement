=============================================
Author: Ascendion AAVA
Date: 
Description: Comprehensive functional test cases for BRANCH_SUMMARY_REPORT enhancement with BRANCH_OPERATIONAL_DETAILS integration
=============================================

# Functional Test Cases for BRANCH_SUMMARY_REPORT Enhancement
## Integration of BRANCH_OPERATIONAL_DETAILS Source Table

---

## Test Suite Overview

**Project**: PySpark Code Enhancement for Databricks  
**Epic**: PCE-1 - PySpark Code enhancement for Databricks  
**Story**: PCE-2 - Extend BRANCH_SUMMARY_REPORT Logic to Integrate New Source Table  
**Test Scope**: Functional validation of ETL enhancement for regulatory reporting compliance  
**Test Environment**: Databricks Delta Lake with PySpark ETL pipeline  

---

## Test Case Categories

1. **Schema Validation Tests** (TC_PCE2_001 - TC_PCE2_005)
2. **Data Integration Tests** (TC_PCE2_006 - TC_PCE2_015)
3. **Conditional Logic Tests** (TC_PCE2_016 - TC_PCE2_025)
4. **Backward Compatibility Tests** (TC_PCE2_026 - TC_PCE2_030)
5. **Edge Case and Error Handling Tests** (TC_PCE2_031 - TC_PCE2_040)
6. **Data Quality and Validation Tests** (TC_PCE2_041 - TC_PCE2_050)

---

## 1. Schema Validation Tests

### Test Case ID: TC_PCE2_001
**Title:** Validate BRANCH_SUMMARY_REPORT schema contains new REGION column
**Description:** Verify that the target Delta table schema includes the new REGION column with correct data type and properties.
**Preconditions:**
- Databricks workspace is accessible
- BRANCH_SUMMARY_REPORT table exists in workspace.default schema
- Schema migration has been executed
**Steps to Execute:**
1. Connect to Databricks workspace
2. Execute: `DESCRIBE workspace.default.branch_summary_report`
3. Verify REGION column exists in the schema output
4. Validate REGION column data type is STRING
5. Check column is nullable (allows NULL values)
**Expected Result:**
- REGION column is present in table schema
- Data type is STRING
- Column allows NULL values
- No schema validation errors
**Linked Jira Ticket:** PCE-2

### Test Case ID: TC_PCE2_002
**Title:** Validate BRANCH_SUMMARY_REPORT schema contains new LAST_AUDIT_DATE column
**Description:** Verify that the target Delta table schema includes the new LAST_AUDIT_DATE column with correct data type and properties.
**Preconditions:**
- Databricks workspace is accessible
- BRANCH_SUMMARY_REPORT table exists in workspace.default schema
- Schema migration has been executed
**Steps to Execute:**
1. Connect to Databricks workspace
2. Execute: `DESCRIBE workspace.default.branch_summary_report`
3. Verify LAST_AUDIT_DATE column exists in the schema output
4. Validate LAST_AUDIT_DATE column data type is STRING
5. Check column is nullable (allows NULL values)
**Expected Result:**
- LAST_AUDIT_DATE column is present in table schema
- Data type is STRING
- Column allows NULL values
- No schema validation errors
**Linked Jira Ticket:** PCE-2

### Test Case ID: TC_PCE2_003
**Title:** Validate BRANCH_OPERATIONAL_DETAILS source table schema
**Description:** Verify that the new source table has the correct schema structure with all required columns and constraints.
**Preconditions:**
- Oracle database connection is available
- BRANCH_OPERATIONAL_DETAILS table has been created
- Database user has SELECT privileges on the table
**Steps to Execute:**
1. Connect to Oracle source database
2. Execute: `DESC BRANCH_OPERATIONAL_DETAILS`
3. Verify all required columns exist: BRANCH_ID