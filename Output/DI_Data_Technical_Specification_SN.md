=============================================
Author: Ascendion AAVA
Date: 
Description: Technical specification for integrating BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT ETL pipeline
=============================================

# Technical Specification for BRANCH_OPERATIONAL_DETAILS Integration Enhancement

## Introduction

### Project Overview
This technical specification outlines the enhancement required to integrate the new Oracle source table `BRANCH_OPERATIONAL_DETAILS` into the existing ETL pipeline for regulatory reporting. The enhancement will extend the `BRANCH_SUMMARY_REPORT` logic to include branch-level operational metadata for improved compliance and audit readiness.

### Business Context
As per JIRA stories PCE-1 and PCE-2, the business requirement is to improve compliance and audit readiness by incorporating branch operational metadata (region, manager name, audit date, active status) into the core reporting layer. This ensures that audit trails and compliance checks include contextual metadata.

### Scope
- Integration of `BRANCH_OPERATIONAL_DETAILS` table into existing ETL pipeline
- Enhancement of `BRANCH_SUMMARY_REPORT` with new columns: `REGION` and `LAST_AUDIT_DATE`
- Modification of PySpark ETL logic to handle the new data source
- Maintenance of backward compatibility with existing records

## Code Changes

### 1. Enhanced ETL Function - create_branch_summary_report

#### Current Implementation
```python
def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, branch_df: DataFrame) -> DataFrame:
    logger.info("Creating Branch Summary Report DataFrame.")
    return transaction_df.join(account_df, "ACCOUNT_ID") \
                         .join(branch_df, "BRANCH_ID") \
                         .groupBy("BRANCH_ID", "BRANCH_NAME") \
                         .agg(
                             count("*").alias("TOTAL_TRANSACTIONS"),
                             sum("AMOUNT").alias("TOTAL_AMOUNT")
                         )
```

#### Enhanced Implementation
```python
def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, 
                               branch_df: DataFrame, branch_operational_df: DataFrame) -> DataFrame:
    """
    Creates the BRANCH_SUMMARY_REPORT DataFrame by aggregating transaction data at the branch level
    and incorporating operational metadata from BRANCH_OPERATIONAL_DETAILS.

    :param transaction_df: DataFrame with transaction data.
    :param account_df: DataFrame with account data.
    :param branch_df: DataFrame with branch data.
    :param branch_operational_df: DataFrame with branch operational details.
    :return: A DataFrame containing the enhanced branch summary report.
    """
    logger.info("Creating Enhanced Branch Summary Report DataFrame with operational details.")
    
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
    
    # Left join with operational details to maintain all branches
    enhanced_summary = base_summary.join(
        active_branch_operational_df.select("BRANCH_ID", "REGION", "LAST_AUDIT_DATE"),
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

### 2. Main Function Enhancement

#### Additional Code Changes Required
```python
def main():
    spark = None
    try:
        spark = get_spark_session()

        # JDBC connection properties (existing)
        jdbc_url = "jdbc:oracle:thin:@your_oracle_host:1521:orcl"
        connection_properties = {
            "user": "your_user",
            "password": "your_password",
            "driver": "oracle.jdbc.driver.OracleDriver"
        }

        # Read source tables (existing + new)
        customer_df = read_table(spark, jdbc_url, "CUSTOMER", connection_properties)
        account_df = read_table(spark, jdbc_url, "ACCOUNT", connection_properties)
        transaction_df = read_table(spark, jdbc_url, "TRANSACTION", connection_properties)
        branch_df = read_table(spark, jdbc_url, "BRANCH", connection_properties)
        
        # NEW: Read branch operational details
        branch_operational_df = read_table(spark, jdbc_url, "BRANCH_OPERATIONAL_DETAILS", connection_properties)

        # Create and write AML_CUSTOMER_TRANSACTIONS (unchanged)
        aml_transactions_df = create_aml_customer_transactions(customer_df, account_df, transaction_df)
        write_to_delta_table(aml_transactions_df, "AML_CUSTOMER_TRANSACTIONS")

        # ENHANCED: Create and write BRANCH_SUMMARY_REPORT with operational details
        branch_summary_df = create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_df)
        write_to_delta_table(branch_summary_df, "BRANCH_SUMMARY_REPORT")

        logger.info("Enhanced ETL job completed successfully.")

    except Exception as e:
        logger.error(f"ETL job failed with exception: {e}")
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped.")
```

### 3. Import Statements Enhancement
No additional imports required as the existing PySpark functions are sufficient for the enhancement.

## Data Model Updates

### Source Data Model Changes

#### New Source Table: BRANCH_OPERATIONAL_DETAILS
```sql
CREATE TABLE BRANCH_OPERATIONAL_DETAILS (
    BRANCH_ID INT PRIMARY KEY,
    REGION VARCHAR2(50) NOT NULL,
    MANAGER_NAME VARCHAR2(100),
    LAST_AUDIT_DATE DATE,
    IS_ACTIVE CHAR(1) DEFAULT 'Y'
);
```

**Key Characteristics:**
- Primary key: `BRANCH_ID`
- Relationship: One-to-one with `BRANCH` table
- Filter condition: `IS_ACTIVE = 'Y'` for active branches only

### Target Data Model Changes

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

**Schema Evolution:**
- Added `REGION` column (STRING, nullable)
- Added `LAST_AUDIT_DATE` column (STRING, nullable)
- Maintains backward compatibility with existing records

## Source-to-Target Mapping

### Field Mapping Table

| Source Table | Source Column | Target Table | Target Column | Transformation Rule | Data Type | Nullable |
|--------------|---------------|--------------|---------------|-------------------|-----------|----------|
| TRANSACTION | TRANSACTION_ID | BRANCH_SUMMARY_REPORT | TOTAL_TRANSACTIONS | COUNT(*) aggregation | BIGINT | No |
| TRANSACTION | AMOUNT | BRANCH_SUMMARY_REPORT | TOTAL_AMOUNT | SUM(AMOUNT) aggregation | DOUBLE | No |
| BRANCH | BRANCH_ID | BRANCH_SUMMARY_REPORT | BRANCH_ID | Direct mapping | BIGINT | No |
| BRANCH | BRANCH_NAME | BRANCH_SUMMARY_REPORT | BRANCH_NAME | Direct mapping | STRING | No |
| BRANCH_OPERATIONAL_DETAILS | REGION | BRANCH_SUMMARY_REPORT | REGION | Direct mapping (filtered by IS_ACTIVE='Y') | STRING | Yes |
| BRANCH_OPERATIONAL_DETAILS | LAST_AUDIT_DATE | BRANCH_SUMMARY_REPORT | LAST_AUDIT_DATE | Cast to STRING (filtered by IS_ACTIVE='Y') | STRING | Yes |

### Transformation Rules

#### 1. Active Branch Filter
```python
# Filter condition for operational details
active_branch_operational_df = branch_operational_df.filter(col("IS_ACTIVE") == "Y")
```

#### 2. Date Type Conversion
```python
# Convert DATE to STRING for target compatibility
col("LAST_AUDIT_DATE").cast("string").alias("LAST_AUDIT_DATE")
```

#### 3. Left Join Strategy
```python
# Maintain all branches, even those without operational details
enhanced_summary = base_summary.join(
    active_branch_operational_df.select("BRANCH_ID", "REGION", "LAST_AUDIT_DATE"),
    "BRANCH_ID",
    "left"  # Left join to preserve all branches
)
```

### Data Flow Diagram
```
TRANSACTION ──┐
              ├── JOIN (ACCOUNT_ID) ──┐
ACCOUNT ──────┘                      ├── GROUP BY (BRANCH_ID, BRANCH_NAME)
                                     │
BRANCH ───────────────────────────────┤
                                     │
BRANCH_OPERATIONAL_DETAILS ──────────┴── LEFT JOIN (BRANCH_ID) ──> BRANCH_SUMMARY_REPORT
(filtered: IS_ACTIVE='Y')
```

## Assumptions and Constraints

### Assumptions
1. **Data Quality**: `BRANCH_OPERATIONAL_DETAILS.BRANCH_ID` exists in the `BRANCH` table
2. **Active Status**: Only branches with `IS_ACTIVE = 'Y'` should contribute operational metadata
3. **Backward Compatibility**: Existing branches without operational details should still appear in the report with NULL values for new columns
4. **Data Refresh**: Full reload of `BRANCH_SUMMARY_REPORT` is acceptable for this enhancement
5. **Oracle Connectivity**: Existing JDBC connection can access the new `BRANCH_OPERATIONAL_DETAILS` table

### Constraints
1. **Performance**: Left join strategy may impact performance for large datasets
2. **Data Types**: `LAST_AUDIT_DATE` must be converted from DATE to STRING for Delta table compatibility
3. **Null Handling**: New columns will contain NULL values for branches without operational details
4. **Schema Evolution**: Delta table schema evolution must be enabled for seamless deployment

### Technical Considerations
1. **Memory Usage**: Additional table join may increase memory consumption
2. **Processing Time**: Enhanced logic may increase overall ETL execution time
3. **Error Handling**: Existing error handling mechanisms will cover the new table read operations
4. **Logging**: Enhanced logging messages added for operational details processing

## References

### JIRA Stories
- **PCE-1**: PySpark Code enhancement for Databricks
- **PCE-2**: Extend BRANCH_SUMMARY_REPORT Logic to Integrate New Source Table

### Source Files
- **ETL Code**: `Input/RegulatoryReportingETL.py`
- **Source DDL**: `Input/Source_DDL.txt`
- **Target DDL**: `Input/Target_DDL.txt`
- **Confluence Context**: `Input/confluence_content.txt`
- **Branch Operational DDL**: `Input/branch_operational_details.sql`

### Technical Documentation
- **Delta Lake Documentation**: For schema evolution and table properties
- **PySpark SQL Functions**: For join operations and data transformations
- **Oracle JDBC Driver**: For database connectivity requirements

---

**Document Version**: 1.0  
**Last Updated**: Current Date  
**Review Status**: Ready for Technical Review