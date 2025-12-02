=============================================
Author: Ascendion AAVA
Date: 
Description: Technical specification for integrating BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT ETL pipeline
=============================================

# Technical Specification for BRANCH_SUMMARY_REPORT Enhancement

## Introduction

### Purpose
This technical specification outlines the required changes to integrate the new Oracle source table `BRANCH_OPERATIONAL_DETAILS` into the existing ETL pipeline that generates the `BRANCH_SUMMARY_REPORT` in Databricks Delta format.

### Business Context
As part of regulatory reporting enhancements, branch-level operational metadata (region, manager name, audit date, active status) needs to be incorporated into the core reporting layer to ensure audit trails and compliance checks include contextual metadata.

### Scope
- Integration of `BRANCH_OPERATIONAL_DETAILS` source table
- Enhancement of existing PySpark ETL logic
- Schema updates to `BRANCH_SUMMARY_REPORT` target table
- Backward compatibility maintenance

### JIRA References
- **PCE-1**: PySpark Code enhancement for Databricks
- **PCE-2**: Extend BRANCH_SUMMARY_REPORT Logic to Integrate New Source Table

## Code Changes

### 1. Enhanced Data Reading Function

**File**: `RegulatoryReportingETL.py`
**Function**: `main()`

**Current Code Location**: Lines 89-94
```python
# Read source tables
customer_df = read_table(spark, jdbc_url, "CUSTOMER", connection_properties)
account_df = read_table(spark, jdbc_url, "ACCOUNT", connection_properties)
transaction_df = read_table(spark, jdbc_url, "TRANSACTION", connection_properties)
branch_df = read_table(spark, jdbc_url, "BRANCH", connection_properties)
```

**Enhanced Code**:
```python
# Read source tables
customer_df = read_table(spark, jdbc_url, "CUSTOMER", connection_properties)
account_df = read_table(spark, jdbc_url, "ACCOUNT", connection_properties)
transaction_df = read_table(spark, jdbc_url, "TRANSACTION", connection_properties)
branch_df = read_table(spark, jdbc_url, "BRANCH", connection_properties)
branch_operational_df = read_table(spark, jdbc_url, "BRANCH_OPERATIONAL_DETAILS", connection_properties)
```

### 2. Modified Branch Summary Report Function

**Function**: `create_branch_summary_report()`
**Current Signature**: `create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, branch_df: DataFrame) -> DataFrame`

**Enhanced Signature**:
```python
def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, 
                               branch_df: DataFrame, branch_operational_df: DataFrame) -> DataFrame:
```

**Enhanced Function Implementation**:
```python
def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, 
                               branch_df: DataFrame, branch_operational_df: DataFrame) -> DataFrame:
    """
    Creates the BRANCH_SUMMARY_REPORT DataFrame by aggregating transaction data at the branch level
    and incorporating operational details.

    :param transaction_df: DataFrame with transaction data.
    :param account_df: DataFrame with account data.
    :param branch_df: DataFrame with branch data.
    :param branch_operational_df: DataFrame with branch operational details.
    :return: A DataFrame containing the enhanced branch summary report.
    """
    logger.info("Creating Enhanced Branch Summary Report DataFrame.")
    
    # Filter active branches only
    active_branch_operational_df = branch_operational_df.filter(col("IS_ACTIVE") == "Y")
    
    # Create base aggregation
    base_summary = transaction_df.join(account_df, "ACCOUNT_ID") \
                                .join(branch_df, "BRANCH_ID") \
                                .groupBy("BRANCH_ID", "BRANCH_NAME") \
                                .agg(
                                    count("*").alias("TOTAL_TRANSACTIONS"),
                                    sum("AMOUNT").alias("TOTAL_AMOUNT")
                                )
    
    # Left join with operational details to preserve all branches
    enhanced_summary = base_summary.join(
        active_branch_operational_df.select(
            col("BRANCH_ID"),
            col("REGION"),
            col("LAST_AUDIT_DATE")
        ),
        "BRANCH_ID",
        "left"
    )
    
    return enhanced_summary
```

### 3. Updated Main Function Call

**Current Code Location**: Lines 99-100
```python
# Create and write BRANCH_SUMMARY_REPORT
branch_summary_df = create_branch_summary_report(transaction_df, account_df, branch_df)
```

**Enhanced Code**:
```python
# Create and write BRANCH_SUMMARY_REPORT
branch_summary_df = create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_df)
```

### 4. Additional Import Requirements

**Add to imports section**:
```python
from pyspark.sql.functions import col, count, sum, when, lit
```

## Data Model Updates

### Source Data Model Changes

#### New Source Table: BRANCH_OPERATIONAL_DETAILS

| Column Name | Data Type | Constraints | Description |
|-------------|-----------|-------------|-------------|
| BRANCH_ID | INT | PRIMARY KEY | Branch identifier (FK to BRANCH table) |
| REGION | VARCHAR2(50) | NOT NULL | Geographic region of the branch |
| MANAGER_NAME | VARCHAR2(100) | | Name of the branch manager |
| LAST_AUDIT_DATE | DATE | | Date of the last audit conducted |
| IS_ACTIVE | CHAR(1) | DEFAULT 'Y' | Active status flag (Y/N) |

### Target Data Model Changes

#### Enhanced BRANCH_SUMMARY_REPORT Schema

**Current Schema**:
```sql
CREATE TABLE workspace.default.branch_summary_report (
    BRANCH_ID BIGINT,
    BRANCH_NAME STRING,
    TOTAL_TRANSACTIONS BIGINT,
    TOTAL_AMOUNT DOUBLE
)
```

**Enhanced Schema**:
```sql
CREATE TABLE workspace.default.branch_summary_report (
    BRANCH_ID BIGINT,
    BRANCH_NAME STRING,
    TOTAL_TRANSACTIONS BIGINT,
    TOTAL_AMOUNT DOUBLE,
    REGION STRING,
    LAST_AUDIT_DATE STRING
)
USING delta
TBLPROPERTIES (
    'delta.enableDeletionVectors' = 'true',
    'delta.feature.appendOnly' = 'supported',
    'delta.feature.deletionVectors' = 'supported',
    'delta.feature.invariants' = 'supported',
    'delta.minReaderVersion' = '3',
    'delta.minWriterVersion' = '7',
    'delta.parquet.compression.codec' = 'zstd'
);
```

**Schema Migration Strategy**:
1. Add new columns to existing Delta table using ALTER TABLE statements
2. Perform full reload to populate historical data
3. Implement backward compatibility checks

## Source-to-Target Mapping

### Enhanced Field Mapping

| Source Table | Source Column | Target Table | Target Column | Transformation Rule | Data Type Conversion |
|--------------|---------------|--------------|---------------|-------------------|---------------------|
| TRANSACTION | COUNT(*) | BRANCH_SUMMARY_REPORT | TOTAL_TRANSACTIONS | Aggregation by BRANCH_ID | INT → BIGINT |
| TRANSACTION | SUM(AMOUNT) | BRANCH_SUMMARY_REPORT | TOTAL_AMOUNT | Aggregation by BRANCH_ID | DECIMAL(15,2) → DOUBLE |
| BRANCH | BRANCH_ID | BRANCH_SUMMARY_REPORT | BRANCH_ID | Direct mapping | INT → BIGINT |
| BRANCH | BRANCH_NAME | BRANCH_SUMMARY_REPORT | BRANCH_NAME | Direct mapping | STRING → STRING |
| BRANCH_OPERATIONAL_DETAILS | REGION | BRANCH_SUMMARY_REPORT | REGION | Conditional mapping (IS_ACTIVE = 'Y') | VARCHAR2(50) → STRING |
| BRANCH_OPERATIONAL_DETAILS | LAST_AUDIT_DATE | BRANCH_SUMMARY_REPORT | LAST_AUDIT_DATE | Conditional mapping (IS_ACTIVE = 'Y') | DATE → STRING |

### Transformation Rules

#### 1. Region Mapping
```sql
CASE 
    WHEN BRANCH_OPERATIONAL_DETAILS.IS_ACTIVE = 'Y' 
    THEN BRANCH_OPERATIONAL_DETAILS.REGION 
    ELSE NULL 
END AS REGION
```

#### 2. Last Audit Date Mapping
```sql
CASE 
    WHEN BRANCH_OPERATIONAL_DETAILS.IS_ACTIVE = 'Y' 
    THEN CAST(BRANCH_OPERATIONAL_DETAILS.LAST_AUDIT_DATE AS STRING)
    ELSE NULL 
END AS LAST_AUDIT_DATE
```

#### 3. Join Logic
```sql
LEFT JOIN BRANCH_OPERATIONAL_DETAILS 
    ON BRANCH.BRANCH_ID = BRANCH_OPERATIONAL_DETAILS.BRANCH_ID 
    AND BRANCH_OPERATIONAL_DETAILS.IS_ACTIVE = 'Y'
```

### Data Quality Rules

1. **Null Handling**: New columns (REGION, LAST_AUDIT_DATE) can be NULL for branches without operational details
2. **Active Status Filter**: Only include operational details for branches with IS_ACTIVE = 'Y'
3. **Data Type Consistency**: Ensure proper casting from Oracle DATE to Databricks STRING format
4. **Referential Integrity**: Maintain BRANCH_ID consistency across all joined tables

## Assumptions and Constraints

### Assumptions
1. `BRANCH_OPERATIONAL_DETAILS` table exists in the Oracle source database
2. BRANCH_ID serves as the primary key in `BRANCH_OPERATIONAL_DETAILS`
3. All branches in the `BRANCH` table may not have corresponding records in `BRANCH_OPERATIONAL_DETAILS`
4. The IS_ACTIVE flag accurately represents the current operational status
5. Oracle JDBC driver is available in the Spark classpath

### Constraints
1. **Backward Compatibility**: Existing records without operational details must be preserved
2. **Performance**: Left join operation should not significantly impact ETL performance
3. **Data Governance**: New columns must follow existing naming conventions
4. **Security**: Database credentials must be managed securely (not hardcoded)
5. **Deployment**: Requires full reload of `BRANCH_SUMMARY_REPORT` table

### Technical Constraints
1. **Schema Evolution**: Delta table schema changes require proper versioning
2. **Data Types**: Oracle DATE to Databricks STRING conversion for compatibility
3. **Memory Management**: Additional table join may increase memory requirements
4. **Error Handling**: Robust exception handling for new table read operations

## References

### Documentation
- JIRA Story PCE-2: Extend BRANCH_SUMMARY_REPORT Logic to Integrate New Source Table
- Confluence Page: ETL Change - Integration of BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT
- Existing ETL Code: `RegulatoryReportingETL.py`

### Database Objects
- Source Table: `BRANCH_OPERATIONAL_DETAILS` (Oracle)
- Target Table: `workspace.default.branch_summary_report` (Databricks Delta)
- Related Tables: `BRANCH`, `ACCOUNT`, `TRANSACTION`, `CUSTOMER`

### Technical Standards
- PySpark DataFrame API
- Databricks Delta Lake format
- Oracle JDBC connectivity
- Python logging framework

---

**Document Version**: 1.0  
**Last Updated**: Current Date  
**Review Status**: Pending Technical Review  
**Approval Status**: Pending Stakeholder Approval