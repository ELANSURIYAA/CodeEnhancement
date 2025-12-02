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
- Integration of `BRANCH_OPERATIONAL_DETAILS` table into existing PySpark ETL pipeline
- Schema updates to `BRANCH_SUMMARY_REPORT` target table
- Conditional data population based on active status
- Maintenance of backward compatibility

## Code Changes

### 1. Enhanced Table Reading Function

**File:** `RegulatoryReportingETL.py`

**Current Implementation:**
```python
# Read source tables
customer_df = read_table(spark, jdbc_url, "CUSTOMER", connection_properties)
account_df = read_table(spark, jdbc_url, "ACCOUNT", connection_properties)
transaction_df = read_table(spark, jdbc_url, "TRANSACTION", connection_properties)
branch_df = read_table(spark, jdbc_url, "BRANCH", connection_properties)
```

**Enhanced Implementation:**
```python
# Read source tables
customer_df = read_table(spark, jdbc_url, "CUSTOMER", connection_properties)
account_df = read_table(spark, jdbc_url, "ACCOUNT", connection_properties)
transaction_df = read_table(spark, jdbc_url, "TRANSACTION", connection_properties)
branch_df = read_table(spark, jdbc_url, "BRANCH", connection_properties)
branch_operational_df = read_table(spark, jdbc_url, "BRANCH_OPERATIONAL_DETAILS", connection_properties)
```

### 2. Updated Branch Summary Report Function

**Current Function:**
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

**Enhanced Function:**
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
    
    # Join with operational details (left join to maintain all branches)
    enhanced_summary = base_summary.join(
        active_branch_operational_df.select(
            "BRANCH_ID", 
            "REGION", 
            "LAST_AUDIT_DATE"
        ), 
        "BRANCH_ID", 
        "left"
    )
    
    return enhanced_summary
```

### 3. Updated Main Function

**Enhanced Main Function:**
```python
def main():
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
        branch_operational_df = read_table(spark, jdbc_url, "BRANCH_OPERATIONAL_DETAILS", connection_properties)

        # Create and write AML_CUSTOMER_TRANSACTIONS
        aml_transactions_df = create_aml_customer_transactions(customer_df, account_df, transaction_df)
        write_to_delta_table(aml_transactions_df, "AML_CUSTOMER_TRANSACTIONS")

        # Create and write enhanced BRANCH_SUMMARY_REPORT
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

## Data Model Updates

### Source Data Model Changes

#### New Source Table: BRANCH_OPERATIONAL_DETAILS
```sql
CREATE TABLE BRANCH_OPERATIONAL_DETAILS (
    BRANCH_ID INT,
    REGION VARCHAR2(50),
    MANAGER_NAME VARCHAR2(100),
    LAST_AUDIT_DATE DATE,
    IS_ACTIVE CHAR(1),
    PRIMARY KEY (BRANCH_ID)
);
```

**Key Characteristics:**
- Primary key: `BRANCH_ID`
- Relationship: One-to-one with `BRANCH` table
- Filter condition: `IS_ACTIVE = 'Y'` for active branches only

### Target Data Model Changes

#### Updated Target Table: BRANCH_SUMMARY_REPORT

**Current Schema:**
```sql
CREATE TABLE workspace.default.branch_summary_report (
    BRANCH_ID BIGINT,
    BRANCH_NAME STRING,
    TOTAL_TRANSACTIONS BIGINT,
    TOTAL_AMOUNT DOUBLE
)
```

**Enhanced Schema:**
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

**New Columns Added:**
- `REGION STRING`: Branch operational region
- `LAST_AUDIT_DATE STRING`: Last audit date for compliance tracking

## Source-to-Target Mapping

### Field Mapping Table

| Source Table | Source Column | Target Table | Target Column | Transformation Rule | Data Type | Nullable |
|--------------|---------------|--------------|---------------|-------------------|-----------|----------|
| TRANSACTION | TRANSACTION_ID | BRANCH_SUMMARY_REPORT | TOTAL_TRANSACTIONS | COUNT(*) aggregation | BIGINT | No |
| TRANSACTION | AMOUNT | BRANCH_SUMMARY_REPORT | TOTAL_AMOUNT | SUM(AMOUNT) aggregation | DOUBLE | No |
| BRANCH | BRANCH_ID | BRANCH_SUMMARY_REPORT | BRANCH_ID | Direct mapping | BIGINT | No |
| BRANCH | BRANCH_NAME | BRANCH_SUMMARY_REPORT | BRANCH_NAME | Direct mapping | STRING | No |
| BRANCH_OPERATIONAL_DETAILS | REGION | BRANCH_SUMMARY_REPORT | REGION | Direct mapping with IS_ACTIVE='Y' filter | STRING | Yes |
| BRANCH_OPERATIONAL_DETAILS | LAST_AUDIT_DATE | BRANCH_SUMMARY_REPORT | LAST_AUDIT_DATE | Direct mapping with IS_ACTIVE='Y' filter, DATE to STRING conversion | STRING | Yes |

### Transformation Rules

#### 1. Active Branch Filter
```sql
WHERE BRANCH_OPERATIONAL_DETAILS.IS_ACTIVE = 'Y'
```

#### 2. Left Join Logic
```python
# Left join to maintain all branches, even those without operational details
enhanced_summary = base_summary.join(
    active_branch_operational_df.select("BRANCH_ID", "REGION", "LAST_AUDIT_DATE"), 
    "BRANCH_ID", 
    "left"
)
```

#### 3. Data Type Conversions
- `LAST_AUDIT_DATE`: Oracle DATE → Databricks STRING
- `REGION`: Oracle VARCHAR2(50) → Databricks STRING

### Join Strategy

```
TRANSACTION
    ↓ (INNER JOIN on ACCOUNT_ID)
ACCOUNT
    ↓ (INNER JOIN on BRANCH_ID)
BRANCH
    ↓ (LEFT JOIN on BRANCH_ID)
BRANCH_OPERATIONAL_DETAILS (WHERE IS_ACTIVE = 'Y')
    ↓
BRANCH_SUMMARY_REPORT
```

## Assumptions and Constraints

### Assumptions
1. **Data Availability**: `BRANCH_OPERATIONAL_DETAILS` table is available in the Oracle source system
2. **Network Connectivity**: Existing JDBC connectivity can access the new table
3. **Data Quality**: `BRANCH_ID` in `BRANCH_OPERATIONAL_DETAILS` matches existing `BRANCH_ID` values
4. **Active Status**: Only branches with `IS_ACTIVE = 'Y'` should contribute operational metadata
5. **Backward Compatibility**: Existing records without operational details should remain accessible

### Constraints
1. **Performance Impact**: Additional join operation may impact ETL performance
2. **Schema Evolution**: Target table schema change requires full reload
3. **Data Governance**: New columns must comply with existing data governance policies
4. **Testing Requirements**: Comprehensive testing required for data integrity
5. **Deployment Coordination**: Requires coordinated deployment of source table and ETL changes

### Risk Mitigation
1. **Performance**: Monitor ETL execution time and optimize join strategies if needed
2. **Data Quality**: Implement data validation checks for new columns
3. **Rollback Plan**: Maintain ability to revert to previous ETL version if issues arise
4. **Monitoring**: Enhanced logging for new join operations and data population

## References

### JIRA Stories
- **PCE-1**: PySpark Code enhancement for Databricks
- **PCE-2**: Extend BRANCH_SUMMARY_REPORT Logic to Integrate New Source Table

### Technical Documentation
- Source DDL: `Input/Source_DDL.txt`
- Target DDL: `Input/Target_DDL.txt`
- Current ETL Implementation: `Input/RegulatoryReportingETL.py`
- Confluence Documentation: `Input/confluence_content.txt`

### Deployment Notes
- **Full Reload Required**: `BRANCH_SUMMARY_REPORT` table requires complete refresh due to schema changes
- **Testing Environment**: Validate changes in development environment before production deployment
- **Data Validation**: Implement reconciliation checks to ensure data accuracy post-deployment

---

**Document Version**: 1.0  
**Last Updated**: Current Date  
**Review Status**: Pending Technical Review