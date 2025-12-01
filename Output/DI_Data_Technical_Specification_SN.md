# Technical Specification for BRANCH_SUMMARY_REPORT Enhancement

=============================================
Author: Ascendion AAVA
Date: 
Description: Technical specification for integrating BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT ETL pipeline
=============================================

## 1. Introduction

### 1.1 Purpose
This document provides a comprehensive technical specification for enhancing the existing ETL pipeline to integrate the new Oracle source table `BRANCH_OPERATIONAL_DETAILS` into the `BRANCH_SUMMARY_REPORT` target table in Databricks Delta Lake.

### 1.2 Business Context
As part of regulatory reporting enhancements, branch-level operational metadata (region, manager name, audit date, active status) needs to be incorporated into the core reporting layer to improve compliance and audit readiness.

### 1.3 Scope
- Integration of `BRANCH_OPERATIONAL_DETAILS` source table
- Enhancement of PySpark ETL logic in `RegulatoryReportingETL.py`
- Schema updates to `BRANCH_SUMMARY_REPORT` target table
- Source-to-target mapping with conditional transformation rules

## 2. Code Changes Required for the Enhancement

### 2.1 Main ETL Function Updates

#### 2.1.1 Modified `main()` Function
```python
def main():
    """
    Main ETL execution function with BRANCH_OPERATIONAL_DETAILS integration.
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

        # Read source tables (EXISTING)
        customer_df = read_table(spark, jdbc_url, "CUSTOMER", connection_properties)
        account_df = read_table(spark, jdbc_url, "ACCOUNT", connection_properties)
        transaction_df = read_table(spark, jdbc_url, "TRANSACTION", connection_properties)
        branch_df = read_table(spark, jdbc_url, "BRANCH", connection_properties)
        
        # NEW: Read BRANCH_OPERATIONAL_DETAILS table
        branch_operational_df = read_table(spark, jdbc_url, "BRANCH_OPERATIONAL_DETAILS", connection_properties)

        # Create and write AML_CUSTOMER_TRANSACTIONS (UNCHANGED)
        aml_transactions_df = create_aml_customer_transactions(customer_df, account_df, transaction_df)
        write_to_delta_table(aml_transactions_df, "AML_CUSTOMER_TRANSACTIONS")

        # MODIFIED: Create and write BRANCH_SUMMARY_REPORT with operational details
        branch_summary_df = create_enhanced_branch_summary_report(
            transaction_df, account_df, branch_df, branch_operational_df
        )
        write_to_delta_table(branch_summary_df, "BRANCH_SUMMARY_REPORT")

        logger.info("ETL job completed successfully.")

    except Exception as e:
        logger.error(f"ETL job failed with exception: {e}")
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped.")
```

#### 2.1.2 New Enhanced Function
```python
def create_enhanced_branch_summary_report(
    transaction_df: DataFrame, 
    account_df: DataFrame, 
    branch_df: DataFrame, 
    branch_operational_df: DataFrame
) -> DataFrame:
    """
    Creates the enhanced BRANCH_SUMMARY_REPORT DataFrame by aggregating transaction data 
    at the branch level and integrating operational details.

    :param transaction_df: DataFrame with transaction data.
    :param account_df: DataFrame with account data.
    :param branch_df: DataFrame with branch data.
    :param branch_operational_df: DataFrame with branch operational details.
    :return: A DataFrame containing the enhanced branch summary report.
    """
    from pyspark.sql.functions import when, lit
    
    logger.info("Creating Enhanced Branch Summary Report DataFrame.")
    
    # Create base branch summary (existing logic)
    base_summary = transaction_df.join(account_df, "ACCOUNT_ID") \
                                .join(branch_df, "BRANCH_ID") \
                                .groupBy("BRANCH_ID", "BRANCH_NAME") \
                                .agg(
                                    count("*").alias("TOTAL_TRANSACTIONS"),
                                    sum("AMOUNT").alias("TOTAL_AMOUNT")
                                )
    
    # Join with operational details and apply conditional logic
    enhanced_summary = base_summary.join(
        branch_operational_df, 
        "BRANCH_ID", 
        "left"  # Left join to maintain all branches
    ).select(
        col("BRANCH_ID"),
        col("BRANCH_NAME"),
        col("TOTAL_TRANSACTIONS"),
        col("TOTAL_AMOUNT"),
        # Conditional population based on IS_ACTIVE = 'Y'
        when(col("IS_ACTIVE") == "Y", col("REGION")).otherwise(lit(None)).alias("REGION"),
        when(col("IS_ACTIVE") == "Y", col("LAST_AUDIT_DATE")).otherwise(lit(None)).alias("LAST_AUDIT_DATE")
    )
    
    return enhanced_summary
```

### 2.2 Import Statement Updates
```python
# Add to existing imports
from pyspark.sql.functions import col, count, sum, when, lit
```

## 3. Data Model Updates

### 3.1 Source Data Model Changes

#### 3.1.1 New Source Table: BRANCH_OPERATIONAL_DETAILS
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
- **Primary Key:** BRANCH_ID
- **Relationship:** One-to-One with BRANCH table
- **Active Filter:** IS_ACTIVE = 'Y' determines data inclusion

### 3.2 Target Data Model Changes

#### 3.2.1 Enhanced BRANCH_SUMMARY_REPORT Schema
```sql
CREATE TABLE workspace.default.branch_summary_report (
    BRANCH_ID BIGINT,
    BRANCH_NAME STRING,
    TOTAL_TRANSACTIONS BIGINT,
    TOTAL_AMOUNT DOUBLE,
    REGION STRING,           -- NEW COLUMN
    LAST_AUDIT_DATE STRING   -- NEW COLUMN
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
- Maintains backward compatibility with existing columns

## 4. Source-to-Target Mapping

### 4.1 Detailed Field Mapping

| Source Table | Source Column | Target Table | Target Column | Transformation Rule | Data Type | Nullable |
|--------------|---------------|--------------|---------------|-------------------|-----------|----------|
| TRANSACTION + ACCOUNT + BRANCH | BRANCH_ID | BRANCH_SUMMARY_REPORT | BRANCH_ID | Direct mapping | BIGINT | No |
| BRANCH | BRANCH_NAME | BRANCH_SUMMARY_REPORT | BRANCH_NAME | Direct mapping | STRING | No |
| TRANSACTION | COUNT(*) | BRANCH_SUMMARY_REPORT | TOTAL_TRANSACTIONS | Aggregation by BRANCH_ID | BIGINT | No |
| TRANSACTION | SUM(AMOUNT) | BRANCH_SUMMARY_REPORT | TOTAL_AMOUNT | Aggregation by BRANCH_ID | DOUBLE | No |
| BRANCH_OPERATIONAL_DETAILS | REGION | BRANCH_SUMMARY_REPORT | REGION | Conditional: IF IS_ACTIVE='Y' THEN REGION ELSE NULL | STRING | Yes |
| BRANCH_OPERATIONAL_DETAILS | LAST_AUDIT_DATE | BRANCH_SUMMARY_REPORT | LAST_AUDIT_DATE | Conditional: IF IS_ACTIVE='Y' THEN LAST_AUDIT_DATE ELSE NULL | STRING | Yes |

### 4.2 Transformation Rules

#### 4.2.1 Join Logic
```sql
-- Conceptual SQL representation of the join logic
SELECT 
    b.BRANCH_ID,
    b.BRANCH_NAME,
    COUNT(t.TRANSACTION_ID) as TOTAL_TRANSACTIONS,
    SUM(t.AMOUNT) as TOTAL_AMOUNT,
    CASE 
        WHEN bod.IS_ACTIVE = 'Y' THEN bod.REGION 
        ELSE NULL 
    END as REGION,
    CASE 
        WHEN bod.IS_ACTIVE = 'Y' THEN bod.LAST_AUDIT_DATE 
        ELSE NULL 
    END as LAST_AUDIT_DATE
FROM TRANSACTION t
JOIN ACCOUNT a ON t.ACCOUNT_ID = a.ACCOUNT_ID
JOIN BRANCH b ON a.BRANCH_ID = b.BRANCH_ID
LEFT JOIN BRANCH_OPERATIONAL_DETAILS bod ON b.BRANCH_ID = bod.BRANCH_ID
GROUP BY b.BRANCH_ID, b.BRANCH_NAME, bod.REGION, bod.LAST_AUDIT_DATE, bod.IS_ACTIVE;
```

#### 4.2.2 Business Rules
1. **Active Branch Filter:** Only populate REGION and LAST_AUDIT_DATE when IS_ACTIVE = 'Y'
2. **Left Join Strategy:** Maintain all branches even if operational details are missing
3. **Null Handling:** Set REGION and LAST_AUDIT_DATE to NULL for inactive branches
4. **Data Type Conversion:** Convert DATE to STRING for LAST_AUDIT_DATE in target

### 4.3 Data Quality Rules

| Rule Type | Description | Implementation |
|-----------|-------------|----------------|
| Referential Integrity | All BRANCH_IDs in operational details must exist in BRANCH table | Left join ensures no data loss |
| Conditional Population | REGION and LAST_AUDIT_DATE only populated for active branches | CASE WHEN IS_ACTIVE='Y' logic |
| Backward Compatibility | Existing records maintain original structure | New columns are nullable |
| Data Completeness | All existing branches preserved in output | Left join strategy |

## 5. Assumptions and Constraints

### 5.1 Technical Assumptions
- Oracle JDBC driver is available in Databricks cluster classpath
- BRANCH_OPERATIONAL_DETAILS table follows the defined schema
- Delta Lake table supports schema evolution for new columns
- Spark session has sufficient memory for join operations

### 5.2 Business Assumptions
- IS_ACTIVE flag accurately represents branch operational status
- BRANCH_ID serves as reliable join key between tables
- LAST_AUDIT_DATE format is consistent in source system
- REGION values follow standardized naming conventions

### 5.3 Constraints
- **Performance:** Additional join may impact ETL execution time
- **Storage:** New columns increase Delta table storage requirements
- **Compatibility:** Requires full reload of BRANCH_SUMMARY_REPORT for schema changes
- **Dependencies:** ETL job depends on availability of BRANCH_OPERATIONAL_DETAILS table

### 5.4 Risk Mitigation
- **Data Validation:** Implement row count and sum validations post-enhancement
- **Rollback Plan:** Maintain backup of existing BRANCH_SUMMARY_REPORT structure
- **Testing:** Comprehensive testing with sample data before production deployment
- **Monitoring:** Add logging for join statistics and null value counts

## 6. Implementation Steps

### 6.1 Pre-Deployment
1. Backup existing BRANCH_SUMMARY_REPORT table
2. Validate BRANCH_OPERATIONAL_DETAILS data quality
3. Test enhanced ETL logic in development environment
4. Perform data reconciliation between old and new logic

### 6.2 Deployment
1. Deploy updated PySpark code to Databricks
2. Execute schema evolution for BRANCH_SUMMARY_REPORT
3. Run full reload ETL job
4. Validate output data quality and completeness

### 6.3 Post-Deployment
1. Monitor ETL job performance metrics
2. Validate business reports using enhanced data
3. Document any issues and resolutions
4. Update operational procedures

## 7. References

### 7.1 JIRA Stories
- **PCE-1:** PySpark Code enhancement for Databricks
- **PCE-2:** Extend BRANCH_SUMMARY_REPORT Logic to Integrate New Source Table

### 7.2 Technical Documentation
- Existing ETL Code: `RegulatoryReportingETL.py`
- Source DDL: `Source_DDL.txt`
- Target DDL: `Target_DDL.txt`
- Confluence Documentation: ETL Change - Integration of BRANCH_OPERATIONAL_DETAILS

### 7.3 Data Models
- Source Oracle Tables: CUSTOMER, ACCOUNT, TRANSACTION, BRANCH, BRANCH_OPERATIONAL_DETAILS
- Target Delta Tables: AML_CUSTOMER_TRANSACTIONS, BRANCH_SUMMARY_REPORT

---

**Document Version:** 1.0  
**Last Updated:** Generated via Ascendion AAVA  
**Review Status:** Pending Technical Review