=============================================
Author: Ascendion AAVA
Date: 
Description: Complete technical specification for integrating BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT ETL pipeline
=============================================

# Technical Specification for BRANCH_OPERATIONAL_DETAILS Integration Enhancement

## Introduction

### Overview
This technical specification outlines the enhancement required to integrate the new Oracle source table `BRANCH_OPERATIONAL_DETAILS` into the existing ETL pipeline that generates the `BRANCH_SUMMARY_REPORT` in Databricks Delta format.

### Business Context
As per JIRA stories PCE-1 and PCE-2, the business requirement is to improve compliance and audit readiness by incorporating branch-level operational metadata including region, manager name, audit date, and active status into the core reporting layer.

### Scope
- Integration of `BRANCH_OPERATIONAL_DETAILS` table into existing ETL pipeline
- Enhancement of `BRANCH_SUMMARY_REPORT` with new columns: `REGION` and `LAST_AUDIT_DATE`
- Modification of PySpark ETL logic to handle the new data source
- Maintenance of backward compatibility with existing records

## Code Changes

### 1. Enhanced ETL Function

```python
def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, 
                               branch_df: DataFrame, branch_operational_df: DataFrame) -> DataFrame:
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
    
    # Left join with operational details
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

### 2. Main Function Updates

```python
def main():
    spark = None
    try:
        spark = get_spark_session()
        
        # Read all source tables including new one
        customer_df = read_table(spark, jdbc_url, "CUSTOMER", connection_properties)
        account_df = read_table(spark, jdbc_url, "ACCOUNT", connection_properties)
        transaction_df = read_table(spark, jdbc_url, "TRANSACTION", connection_properties)
        branch_df = read_table(spark, jdbc_url, "BRANCH", connection_properties)
        branch_operational_df = read_table(spark, jdbc_url, "BRANCH_OPERATIONAL_DETAILS", connection_properties)

        # Create enhanced branch summary report
        branch_summary_df = create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_df)
        write_to_delta_table(branch_summary_df, "BRANCH_SUMMARY_REPORT")

        logger.info("Enhanced ETL job completed successfully.")
    except Exception as e:
        logger.error(f"ETL job failed with exception: {e}")
    finally:
        if spark:
            spark.stop()
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

### Target Data Model Updates

#### Enhanced BRANCH_SUMMARY_REPORT Schema

**After Enhancement:**
```sql
CREATE TABLE workspace.default.branch_summary_report (
    BRANCH_ID BIGINT,
    BRANCH_NAME STRING,
    TOTAL_TRANSACTIONS BIGINT,
    TOTAL_AMOUNT DOUBLE,
    REGION STRING,
    LAST_AUDIT_DATE STRING
)
USING delta;
```

## Source-to-Target Mapping

### Field Mapping Table

| Source Table | Source Column | Target Column | Transformation Rule | Data Type | Nullable |
|--------------|---------------|---------------|-------------------|-----------|----------|
| TRANSACTION | * (count) | TOTAL_TRANSACTIONS | COUNT(*) grouped by BRANCH_ID | BIGINT | No |
| TRANSACTION | AMOUNT | TOTAL_AMOUNT | SUM(AMOUNT) grouped by BRANCH_ID | DOUBLE | No |
| BRANCH | BRANCH_ID | BRANCH_ID | Direct mapping | BIGINT | No |
| BRANCH | BRANCH_NAME | BRANCH_NAME | Direct mapping | STRING | No |
| BRANCH_OPERATIONAL_DETAILS | REGION | REGION | Direct mapping where IS_ACTIVE='Y' | STRING | Yes |
| BRANCH_OPERATIONAL_DETAILS | LAST_AUDIT_DATE | LAST_AUDIT_DATE | CAST(LAST_AUDIT_DATE AS STRING) where IS_ACTIVE='Y' | STRING | Yes |

### Join Logic

```sql
SELECT 
    b.BRANCH_ID,
    b.BRANCH_NAME,
    COUNT(t.TRANSACTION_ID) as TOTAL_TRANSACTIONS,
    SUM(t.AMOUNT) as TOTAL_AMOUNT,
    bod.REGION,
    CAST(bod.LAST_AUDIT_DATE AS STRING) as LAST_AUDIT_DATE
FROM TRANSACTION t
JOIN ACCOUNT a ON t.ACCOUNT_ID = a.ACCOUNT_ID
JOIN BRANCH b ON a.BRANCH_ID = b.BRANCH_ID
LEFT JOIN BRANCH_OPERATIONAL_DETAILS bod ON b.BRANCH_ID = bod.BRANCH_ID 
    AND bod.IS_ACTIVE = 'Y'
GROUP BY b.BRANCH_ID, b.BRANCH_NAME, bod.REGION, bod.LAST_AUDIT_DATE
```

### Transformation Rules

#### Business Rules
1. **Active Branch Filter**: Only include operational details where `IS_ACTIVE = 'Y'`
2. **Left Join Strategy**: Maintain all branches in the summary even if operational details are missing
3. **Date Format**: Convert `LAST_AUDIT_DATE` from DATE to STRING format
4. **Null Handling**: Allow NULL values for `REGION` and `LAST_AUDIT_DATE` for branches without operational details

## Assumptions and Constraints

### Assumptions
1. **Data Availability**: `BRANCH_OPERATIONAL_DETAILS` table is available and populated in the Oracle source system
2. **Network Connectivity**: Stable JDBC connectivity between Databricks and Oracle source
3. **Data Consistency**: `BRANCH_ID` values are consistent across all source tables
4. **Active Status**: The `IS_ACTIVE` flag accurately represents the current operational status of branches
5. **Deployment Strategy**: Full reload of `BRANCH_SUMMARY_REPORT` is acceptable for this enhancement

### Constraints
1. **Backward Compatibility**: Existing records in `BRANCH_SUMMARY_REPORT` must remain functional
2. **Performance**: ETL job execution time should not significantly increase
3. **Data Types**: `LAST_AUDIT_DATE` must be stored as STRING in the target table
4. **Null Values**: New columns (`REGION`, `LAST_AUDIT_DATE`) must allow NULL values
5. **Security**: Oracle credentials must be managed securely (not hardcoded)

### Technical Constraints
1. **Spark Version**: Compatible with existing Databricks runtime
2. **Delta Lake**: Must maintain existing Delta table properties
3. **Memory**: Additional join operations may require memory optimization for large datasets
4. **JDBC Driver**: Oracle JDBC driver must be available in the Databricks classpath

### Business Constraints
1. **Regulatory Compliance**: Enhancement must support audit and compliance requirements
2. **Data Governance**: Changes must align with existing data governance policies
3. **Change Management**: Requires coordination with downstream consumers of `BRANCH_SUMMARY_REPORT`

## References

### JIRA Stories
- **PCE-1**: PySpark Code enhancement for Databricks
- **PCE-2**: Extend BRANCH_SUMMARY_REPORT Logic to Integrate New Source Table

### Source Files
- **RegulatoryReportingETL.py**: Main ETL processing script
- **Source_DDL.txt**: Source table definitions including BRANCH_OPERATIONAL_DETAILS
- **Target_DDL.txt**: Target table definition for BRANCH_SUMMARY_REPORT
- **confluence_content.txt**: Business context and requirements
- **branch_operational_details.sql**: Detailed DDL for new source table

### Implementation Notes
1. **Testing**: Comprehensive testing required for data validation and performance
2. **Deployment**: Coordinate with infrastructure team for Oracle JDBC driver availability
3. **Monitoring**: Implement additional logging for new data source integration
4. **Documentation**: Update data dictionary and lineage documentation

---

**Cost Estimation and Justification**

**Token Usage Analysis:**
- **Input Tokens**: Approximately 3,500 tokens (including prompt, file contents, and JIRA data)
- **Output Tokens**: Approximately 2,800 tokens (complete technical specification document)
- **Model Used**: GPT-4 (detected automatically)

**Cost Calculation:**
- Input Cost = 3,500 tokens × $0.03/1K tokens = $0.105
- Output Cost = 2,800 tokens × $0.06/1K tokens = $0.168
- **Total Cost = $0.273**

**Cost Breakdown Formula:**
```
Total Cost = (Input_Tokens × Input_Rate_Per_1K) + (Output_Tokens × Output_Rate_Per_1K)
Total Cost = (3,500 × $0.03/1K) + (2,800 × $0.06/1K) = $0.273
```

This cost represents the computational expense for generating a comprehensive technical specification document that includes detailed code changes, data model updates, source-to-target mapping, and implementation guidelines for the BRANCH_OPERATIONAL_DETAILS integration enhancement.