=============================================
Author: Ascendion AAVA
Date: 
Description: Technical specification for integrating BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT ETL pipeline
=============================================

# Technical Specification for BRANCH_OPERATIONAL_DETAILS Integration Enhancement

## Introduction

### Purpose
This technical specification outlines the enhancement required to integrate the new Oracle source table `BRANCH_OPERATIONAL_DETAILS` into the existing ETL pipeline for the `BRANCH_SUMMARY_REPORT` target table in Databricks Delta.

### Business Context
As part of regulatory reporting enhancements, branch-level operational metadata (region, manager name, audit date, active status) needs to be incorporated into the core reporting layer to ensure audit trails and compliance checks include contextual metadata.

### Scope
- Integration of `BRANCH_OPERATIONAL_DETAILS` source table
- Enhancement of existing PySpark ETL logic
- Schema updates to `BRANCH_SUMMARY_REPORT` target table
- Source-to-target field mapping with transformation rules

## Code Changes

### 1. Enhanced ETL Function

#### Current Function: `create_branch_summary_report`
**Location:** `RegulatoryReportingETL.py` - Lines 45-58

**Required Changes:**

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
    logger.info("Creating Enhanced Branch Summary Report DataFrame with operational details.")
    
    # Base aggregation
    base_summary = transaction_df.join(account_df, "ACCOUNT_ID") \
                                 .join(branch_df, "BRANCH_ID") \
                                 .groupBy("BRANCH_ID", "BRANCH_NAME") \
                                 .agg(
                                     count("*").alias("TOTAL_TRANSACTIONS"),
                                     sum("AMOUNT").alias("TOTAL_AMOUNT")
                                 )
    
    # Join with operational details (left join to maintain all branches)
    enhanced_summary = base_summary.join(
        branch_operational_df.filter(col("IS_ACTIVE") == "Y"),
        "BRANCH_ID",
        "left"
    ).select(
        col("BRANCH_ID"),
        col("BRANCH_NAME"),
        col("TOTAL_TRANSACTIONS"),
        col("TOTAL_AMOUNT"),
        col("REGION"),
        col("LAST_AUDIT_DATE").cast("string").alias("LAST_AUDIT_DATE")
    )
    
    return enhanced_summary
```

### 2. Main Function Updates

**Location:** `RegulatoryReportingETL.py` - Lines 75-105

**Required Changes:**

```python
def main():
    """
    Main ETL execution function.
    """
    spark = None
    try:
        spark = get_spark_session()

        # JDBC connection properties
        jdbc_url = "jdbc:oracle:thin:@your_oracle_host:1521:orcl"
        connection_properties = {
            "user": "your_user",
            "password": "your_password",
            "driver": "oracle.jdbc.driver.OracleDriver"
        }

        # Read source tables
        customer_df = read_table(spark, jdbc_url, "CUSTOMER", connection_properties)
        account_df = read_table(spark, jdbc_url, "ACCOUNT", connection_properties)
        transaction_df = read_table(spark, jdbc_url, "TRANSACTION", connection_properties)
        branch_df = read_table(spark, jdbc_url, "BRANCH", connection_properties)
        
        # NEW: Read branch operational details
        branch_operational_df = read_table(spark, jdbc_url, "BRANCH_OPERATIONAL_DETAILS", connection_properties)

        # Create and write AML_CUSTOMER_TRANSACTIONS (unchanged)
        aml_transactions_df = create_aml_customer_transactions(customer_df, account_df, transaction_df)
        write_to_delta_table(aml_transactions_df, "AML_CUSTOMER_TRANSACTIONS")

        # Create and write enhanced BRANCH_SUMMARY_REPORT
        branch_summary_df = create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_df)
        write_to_delta_table(branch_summary_df, "BRANCH_SUMMARY_REPORT")

        logger.info("ETL job completed successfully.")

    except Exception as e:
        logger.error(f"ETL job failed with exception: {e}")
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped.")
```

### 3. Import Statements
**Location:** `RegulatoryReportingETL.py` - Lines 1-3

**No changes required** - existing imports are sufficient.

## Data Model Updates

### 1. Source Data Model Enhancement

#### New Source Table: BRANCH_OPERATIONAL_DETAILS
```sql
CREATE TABLE BRANCH_OPERATIONAL_DETAILS (
    BRANCH_ID INT PRIMARY KEY,
    REGION VARCHAR2(50),
    MANAGER_NAME VARCHAR2(100),
    LAST_AUDIT_DATE DATE,
    IS_ACTIVE CHAR(1)
);
```

**Key Characteristics:**
- Primary key: `BRANCH_ID`
- Relationship: One-to-one with `BRANCH` table
- Filter condition: `IS_ACTIVE = 'Y'` for active branches only

### 2. Target Data Model Updates

#### Enhanced BRANCH_SUMMARY_REPORT Schema
```sql
CREATE TABLE workspace.default.branch_summary_report (
    BRANCH_ID BIGINT,
    BRANCH_NAME STRING,
    TOTAL_TRANSACTIONS BIGINT,
    TOTAL_AMOUNT DOUBLE,
    REGION STRING,              -- NEW COLUMN
    LAST_AUDIT_DATE STRING      -- NEW COLUMN
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

**Schema Changes:**
- Added `REGION` column (STRING type)
- Added `LAST_AUDIT_DATE` column (STRING type)
- Maintains existing columns and properties

## Source-to-Target Mapping

### Field Mapping Table

| Source Table | Source Column | Target Table | Target Column | Data Type | Transformation Rules |
|--------------|---------------|--------------|---------------|-----------|---------------------|
| TRANSACTION | TRANSACTION_ID | BRANCH_SUMMARY_REPORT | TOTAL_TRANSACTIONS | BIGINT | COUNT(*) aggregation by BRANCH_ID |
| TRANSACTION | AMOUNT | BRANCH_SUMMARY_REPORT | TOTAL_AMOUNT | DOUBLE | SUM(AMOUNT) aggregation by BRANCH_ID |
| BRANCH | BRANCH_ID | BRANCH_SUMMARY_REPORT | BRANCH_ID | BIGINT | Direct mapping |
| BRANCH | BRANCH_NAME | BRANCH_SUMMARY_REPORT | BRANCH_NAME | STRING | Direct mapping |
| BRANCH_OPERATIONAL_DETAILS | REGION | BRANCH_SUMMARY_REPORT | REGION | STRING | Direct mapping with IS_ACTIVE='Y' filter |
| BRANCH_OPERATIONAL_DETAILS | LAST_AUDIT_DATE | BRANCH_SUMMARY_REPORT | LAST_AUDIT_DATE | STRING | CAST(DATE as STRING) with IS_ACTIVE='Y' filter |

### Transformation Rules

#### 1. Join Logic
```sql
TRANSACTION 
  INNER JOIN ACCOUNT ON TRANSACTION.ACCOUNT_ID = ACCOUNT.ACCOUNT_ID
  INNER JOIN BRANCH ON ACCOUNT.BRANCH_ID = BRANCH.BRANCH_ID
  LEFT JOIN BRANCH_OPERATIONAL_DETAILS ON BRANCH.BRANCH_ID = BRANCH_OPERATIONAL_DETAILS.BRANCH_ID
    AND BRANCH_OPERATIONAL_DETAILS.IS_ACTIVE = 'Y'
```

#### 2. Aggregation Rules
- **TOTAL_TRANSACTIONS:** `COUNT(*)` grouped by `BRANCH_ID`, `BRANCH_NAME`
- **TOTAL_AMOUNT:** `SUM(AMOUNT)` grouped by `BRANCH_ID`, `BRANCH_NAME`

#### 3. Data Type Conversions
- **LAST_AUDIT_DATE:** Convert from Oracle DATE to STRING format
- **BRANCH_ID:** Implicit conversion from INT to BIGINT

#### 4. Null Handling
- **REGION:** NULL if branch not found in operational details or IS_ACTIVE ≠ 'Y'
- **LAST_AUDIT_DATE:** NULL if branch not found in operational details or IS_ACTIVE ≠ 'Y'

## Assumptions and Constraints

### Assumptions
1. **Data Availability:** `BRANCH_OPERATIONAL_DETAILS` table is populated and accessible via existing JDBC connection
2. **Data Quality:** `BRANCH_ID` exists in both `BRANCH` and `BRANCH_OPERATIONAL_DETAILS` tables
3. **Active Status:** Only branches with `IS_ACTIVE = 'Y'` should contribute operational metadata
4. **Backward Compatibility:** Existing records without operational details should remain in the report with NULL values for new columns
5. **Performance:** Left join approach maintains acceptable performance for current data volumes

### Constraints
1. **Schema Evolution:** Target table schema change requires full reload of `BRANCH_SUMMARY_REPORT`
2. **Data Types:** Oracle DATE must be converted to STRING for Databricks Delta compatibility
3. **Join Performance:** Left join may impact performance if `BRANCH_OPERATIONAL_DETAILS` table is large
4. **Null Values:** New columns will contain NULL values for branches without operational details
5. **Deployment:** Requires coordinated deployment of both ETL code and target table schema

### Technical Constraints
1. **Spark Version:** Compatible with existing Spark version used in the pipeline
2. **Delta Lake:** Requires Delta Lake support for schema evolution
3. **JDBC Driver:** Oracle JDBC driver must be available in Spark classpath
4. **Memory:** Additional join operation may require memory tuning for large datasets

## References

### JIRA Stories
- **PCE-1:** PySpark Code enhancement for Databricks
- **PCE-2:** Extend BRANCH_SUMMARY_REPORT Logic to Integrate New Source Table

### Source Files
- `RegulatoryReportingETL.py` - Main ETL pipeline code
- `Source_DDL.txt` - Source table definitions
- `Target_DDL.txt` - Target table schema
- `confluence_content.txt` - Business requirements and context
- `branch_operational_details.sql` - New source table DDL

### Dependencies
- Oracle JDBC Driver
- PySpark SQL functions
- Databricks Delta Lake
- Existing ETL infrastructure

---

**Document Version:** 1.0  
**Last Updated:** Current Date  
**Review Status:** Draft  
**Approval Required:** Technical Lead, Data Architecture Team