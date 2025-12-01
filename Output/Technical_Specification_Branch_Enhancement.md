# Technical Specification for Branch Operational Details Integration

=============================================
**Author:** Ascendion AAVA  
**Date:** 2024-12-19  
**Description:** Technical specification for integrating BRANCH_OPERATIONAL_DETAILS source table into existing BRANCH_SUMMARY_REPORT data model and ETL pipeline  
=============================================

## 1. Introduction

### 1.1 Purpose
This technical specification outlines the integration of the new `BRANCH_OPERATIONAL_DETAILS` source table into the existing regulatory reporting ETL pipeline. The enhancement adds regional and audit metadata to the `BRANCH_SUMMARY_REPORT` target table to support enhanced regulatory compliance and audit trail requirements.

### 1.2 Scope
- Integration of `BRANCH_OPERATIONAL_DETAILS` source table
- Enhancement of existing `create_branch_summary_report` function
- Updates to target `BRANCH_SUMMARY_REPORT` data model
- Source-to-target mapping for new columns: `REGION` and `LAST_AUDIT_DATE`

### 1.3 Business Requirements
As per the regulatory reporting enhancement initiative, branch operational metadata including manager information, regional classification, and audit trail data must be incorporated into core reporting layers to ensure compliance checks include contextual metadata.

## 2. Code Changes Required for the Enhancement

### 2.1 Main ETL Function Updates

#### 2.1.1 Source Table Reading
**File:** `RegulatoryReportingETL.py`  
**Function:** `main()`  
**Location:** Line ~75 (after existing source table reads)

```python
# Add new source table read
branch_operational_df = read_table(spark, jdbc_url, "BRANCH_OPERATIONAL_DETAILS", connection_properties)
```

#### 2.1.2 Function Call Update
**File:** `RegulatoryReportingETL.py`  
**Function:** `main()`  
**Location:** Line ~82 (update existing function call)

```python
# Update function call to include new DataFrame
branch_summary_df = create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_df)
```

### 2.2 Core Business Logic Enhancement

#### 2.2.1 Function Signature Update
**File:** `RegulatoryReportingETL.py`  
**Function:** `create_branch_summary_report`  
**Location:** Line ~45

```python
def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, 
                                branch_df: DataFrame, branch_operational_df: DataFrame) -> DataFrame:
    """
    Creates the BRANCH_SUMMARY_REPORT DataFrame by aggregating transaction data at the branch level
    and enriching with operational details.

    :param transaction_df: DataFrame with transaction data.
    :param account_df: DataFrame with account data.
    :param branch_df: DataFrame with branch data.
    :param branch_operational_df: DataFrame with branch operational details.
    :return: A DataFrame containing the enhanced branch summary report.
    """
```

#### 2.2.2 Enhanced Join Logic
**File:** `RegulatoryReportingETL.py`  
**Function:** `create_branch_summary_report`  
**Location:** Line ~52 (replace existing logic)

```python
logger.info("Creating Enhanced Branch Summary Report DataFrame.")

# Create base aggregation
base_summary = transaction_df.join(account_df, "ACCOUNT_ID") \
                            .join(branch_df, "BRANCH_ID") \
                            .groupBy("BRANCH_ID", "BRANCH_NAME") \
                            .agg(
                                count("*").alias("TOTAL_TRANSACTIONS"),
                                sum("AMOUNT").alias("TOTAL_AMOUNT")
                            )

# Enrich with operational details
enhanced_summary = base_summary.join(branch_operational_df, "BRANCH_ID", "left") \
                               .select(
                                   col("BRANCH_ID"),
                                   col("BRANCH_NAME"),
                                   col("TOTAL_TRANSACTIONS"),
                                   col("TOTAL_AMOUNT"),
                                   col("REGION"),
                                   col("LAST_AUDIT_DATE").cast("string").alias("LAST_AUDIT_DATE")
                               )

return enhanced_summary
```

### 2.3 Import Statements
**File:** `RegulatoryReportingETL.py`  
**Location:** Top of file (no changes required - existing imports sufficient)

## 3. Updates to the Data Models

### 3.1 Source Data Model Analysis

#### 3.1.1 Existing Source Tables
- `CUSTOMER`: Customer master data
- `BRANCH`: Branch master data
- `ACCOUNT`: Account details linking customers to branches
- `TRANSACTION`: Transaction records

#### 3.1.2 New Source Table
**Table:** `BRANCH_OPERATIONAL_DETAILS`

| Column Name | Data Type | Constraints | Description |
|-------------|-----------|-------------|-------------|
| BRANCH_ID | INT | PRIMARY KEY | Branch identifier (FK to BRANCH) |
| REGION | VARCHAR2(50) | NOT NULL | Regional classification |
| MANAGER_NAME | VARCHAR2(100) | | Branch manager name |
| LAST_AUDIT_DATE | DATE | | Date of last audit |
| IS_ACTIVE | CHAR(1) | | Active status flag |

### 3.2 Target Data Model Updates

#### 3.2.1 Current Target Schema
**Table:** `BRANCH_SUMMARY_REPORT`

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| BRANCH_ID | BIGINT | Branch identifier |
| BRANCH_NAME | STRING | Branch name |
| TOTAL_TRANSACTIONS | BIGINT | Count of transactions |
| TOTAL_AMOUNT | DOUBLE | Sum of transaction amounts |

#### 3.2.2 Enhanced Target Schema
**Table:** `BRANCH_SUMMARY_REPORT` (Updated)

| Column Name | Data Type | Description | Change Type |
|-------------|-----------|-------------|-------------|
| BRANCH_ID | BIGINT | Branch identifier | Existing |
| BRANCH_NAME | STRING | Branch name | Existing |
| TOTAL_TRANSACTIONS | BIGINT | Count of transactions | Existing |
| TOTAL_AMOUNT | DOUBLE | Sum of transaction amounts | Existing |
| REGION | STRING | Regional classification | **NEW** |
| LAST_AUDIT_DATE | STRING | Date of last audit | **NEW** |

### 3.3 Data Model Relationships

```
BRANCH (1) ←→ (1) BRANCH_OPERATIONAL_DETAILS
    ↓
ACCOUNT (N)
    ↓
TRANSACTION (N)
    ↓
BRANCH_SUMMARY_REPORT (Aggregated)
```

## 4. Source-to-Target Mapping

### 4.1 Detailed Field Mapping

| Source Table | Source Column | Target Table | Target Column | Transformation Rule | Data Type Conversion |
|--------------|---------------|--------------|---------------|--------------------|-----------------------|
| TRANSACTION | * (count) | BRANCH_SUMMARY_REPORT | TOTAL_TRANSACTIONS | COUNT(*) GROUP BY BRANCH_ID | INT → BIGINT |
| TRANSACTION | AMOUNT | BRANCH_SUMMARY_REPORT | TOTAL_AMOUNT | SUM(AMOUNT) GROUP BY BRANCH_ID | DECIMAL(15,2) → DOUBLE |
| BRANCH | BRANCH_ID | BRANCH_SUMMARY_REPORT | BRANCH_ID | Direct mapping | INT → BIGINT |
| BRANCH | BRANCH_NAME | BRANCH_SUMMARY_REPORT | BRANCH_NAME | Direct mapping | STRING → STRING |
| BRANCH_OPERATIONAL_DETAILS | REGION | BRANCH_SUMMARY_REPORT | REGION | Direct mapping via LEFT JOIN | VARCHAR2(50) → STRING |
| BRANCH_OPERATIONAL_DETAILS | LAST_AUDIT_DATE | BRANCH_SUMMARY_REPORT | LAST_AUDIT_DATE | CAST to STRING | DATE → STRING |

### 4.2 Join Strategy

#### 4.2.1 Primary Aggregation Join
```sql
TRANSACTION 
  INNER JOIN ACCOUNT ON TRANSACTION.ACCOUNT_ID = ACCOUNT.ACCOUNT_ID
  INNER JOIN BRANCH ON ACCOUNT.BRANCH_ID = BRANCH.BRANCH_ID
GROUP BY BRANCH.BRANCH_ID, BRANCH.BRANCH_NAME
```

#### 4.2.2 Operational Details Enrichment Join
```sql
BASE_SUMMARY 
  LEFT JOIN BRANCH_OPERATIONAL_DETAILS ON BASE_SUMMARY.BRANCH_ID = BRANCH_OPERATIONAL_DETAILS.BRANCH_ID
```

### 4.3 Transformation Rules

1. **Date Conversion**: `LAST_AUDIT_DATE` from Oracle DATE to Databricks STRING format
2. **NULL Handling**: LEFT JOIN ensures branches without operational details still appear in report
3. **Data Type Alignment**: Ensure consistent data types between source and target
4. **Regional Classification**: Direct mapping of REGION field without transformation

## 5. Assumptions and Constraints

### 5.1 Assumptions
- `BRANCH_OPERATIONAL_DETAILS` table exists in the source Oracle database
- All branches in `BRANCH` table may not have corresponding records in `BRANCH_OPERATIONAL_DETAILS`
- Oracle JDBC driver is available in the Spark classpath
- Target Delta table supports schema evolution

### 5.2 Constraints
- Requires full reload of `BRANCH_SUMMARY_REPORT` table due to schema changes
- LEFT JOIN strategy ensures no data loss for branches without operational details
- Performance impact due to additional join operation
- Data governance policies must be maintained for new columns

### 5.3 Dependencies
- Oracle database connectivity
- Databricks Delta Lake write permissions
- Spark session with Hive support enabled
- Proper error handling and logging framework

## 6. Implementation Considerations

### 6.1 Performance Optimization
- Consider partitioning strategy for `BRANCH_OPERATIONAL_DETAILS` if table grows large
- Monitor join performance with additional table
- Implement caching strategy for frequently accessed branch operational data

### 6.2 Data Quality Checks
- Validate `BRANCH_ID` referential integrity between `BRANCH` and `BRANCH_OPERATIONAL_DETAILS`
- Implement null checks for critical operational fields
- Add data quality metrics for new columns

### 6.3 Testing Strategy
- Unit tests for enhanced `create_branch_summary_report` function
- Integration tests with sample `BRANCH_OPERATIONAL_DETAILS` data
- End-to-end testing of complete ETL pipeline
- Validation of target table schema and data accuracy

## 7. Deployment Notes

### 7.1 Pre-Deployment
- Backup existing `BRANCH_SUMMARY_REPORT` table
- Verify `BRANCH_OPERATIONAL_DETAILS` table availability
- Update target table schema to include new columns

### 7.2 Deployment Steps
1. Deploy updated ETL code to production environment
2. Execute full reload of `BRANCH_SUMMARY_REPORT`
3. Validate data integrity and completeness
4. Monitor ETL job performance metrics

### 7.3 Post-Deployment
- Verify new columns are populated correctly
- Update downstream reporting and analytics processes
- Document new data lineage and dependencies

## 8. References

- JIRA Stories: PCE-1, PCE-2
- Confluence: ETL Change - Integration of BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT
- Source DDL: `Input/Source_DDL.txt`
- Target DDL: `Input/Target_DDL.txt`
- Current ETL Code: `Input/RegulatoryReportingETL.py`

---

**Document Version:** 1.0  
**Last Updated:** 2024-12-19  
**Review Status:** Draft  
**Approved By:** [Pending Review]