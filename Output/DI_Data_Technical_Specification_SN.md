=============================================
Author: Ascendion AAVA
Date: 
Description: Technical specification for integrating BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT ETL pipeline
=============================================

# Technical Specification for BRANCH_OPERATIONAL_DETAILS Integration Enhancement

## Introduction

This technical specification outlines the required changes to integrate the new Oracle source table `BRANCH_OPERATIONAL_DETAILS` into the existing ETL pipeline for regulatory reporting enhancements. The enhancement will extend the `BRANCH_SUMMARY_REPORT` table to include branch operational metadata (region and last audit date) to improve compliance and audit readiness.

### Business Context
- **JIRA Story**: PCE-2 - Extend BRANCH_SUMMARY_REPORT Logic to Integrate New Source Table
- **Objective**: Incorporate branch-level operational metadata into core reporting layer for audit trails and compliance checks
- **Impact**: Enhanced regulatory reporting with contextual metadata

### Scope
- Integration of `BRANCH_OPERATIONAL_DETAILS` Oracle source table
- Extension of `BRANCH_SUMMARY_REPORT` Delta table with new columns
- Enhancement of PySpark ETL logic in `RegulatoryReportingETL.py`
- Maintenance of backward compatibility

## Code Changes

### 1. ETL Pipeline Modifications

#### 1.1 New Table Reading Function
```python
def read_branch_operational_details(spark: SparkSession, jdbc_url: str, connection_properties: dict) -> DataFrame:
    """
    Reads BRANCH_OPERATIONAL_DETAILS table from Oracle source.
    
    :param spark: The SparkSession object.
    :param jdbc_url: The JDBC URL for the database connection.
    :param connection_properties: Connection properties dictionary.
    :return: DataFrame containing branch operational details.
    """
    logger.info("Reading BRANCH_OPERATIONAL_DETAILS table")
    try:
        df = spark.read.jdbc(
            url=jdbc_url, 
            table="BRANCH_OPERATIONAL_DETAILS", 
            properties=connection_properties
        )
        # Filter only active branches
        active_branches_df = df.filter(col("IS_ACTIVE") == "Y")
        return active_branches_df
    except Exception as e:
        logger.error(f"Failed to read BRANCH_OPERATIONAL_DETAILS: {e}")
        raise
```

#### 1.2 Enhanced Branch Summary Report Function
```python
def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, 
                                branch_df: DataFrame, branch_operational_df: DataFrame) -> DataFrame:
    """
    Creates the BRANCH_SUMMARY_REPORT DataFrame with enhanced operational details.
    
    :param transaction_df: DataFrame with transaction data.
    :param account_df: DataFrame with account data.
    :param branch_df: DataFrame with branch data.
    :param branch_operational_df: DataFrame with branch operational details.
    :return: Enhanced DataFrame containing the branch summary report.
    """
    logger.info("Creating Enhanced Branch Summary Report DataFrame.")
    
    # Base aggregation
    base_summary = transaction_df.join(account_df, "ACCOUNT_ID") \
                                 .join(branch_df, "BRANCH_ID") \
                                 .groupBy("BRANCH_ID", "BRANCH_NAME") \
                                 .agg(
                                     count("*").alias("TOTAL_TRANSACTIONS"),
                                     sum("AMOUNT").alias("TOTAL_AMOUNT")
                                 )
    
    # Left join with operational details to maintain all branches
    enhanced_summary = base_summary.join(
        branch_operational_df.select(
            "BRANCH_ID", 
            "REGION", 
            "LAST_AUDIT_DATE"
        ), 
        "BRANCH_ID", 
        "left"
    )
    
    return enhanced_summary
```

#### 1.3 Main Function Updates
```python
def main():
    """
    Enhanced main ETL execution function.
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
        branch_operational_df = read_branch_operational_details(spark, jdbc_url, connection_properties)

        # Create and write AML_CUSTOMER_TRANSACTIONS (unchanged)
        aml_transactions_df = create_aml_customer_transactions(customer_df, account_df, transaction_df)
        write_to_delta_table(aml_transactions_df, "AML_CUSTOMER_TRANSACTIONS")

        # ENHANCED: Create and write BRANCH_SUMMARY_REPORT with operational details
        branch_summary_df = create_branch_summary_report(
            transaction_df, account_df, branch_df, branch_operational_df
        )
        write_to_delta_table(branch_summary_df, "BRANCH_SUMMARY_REPORT")

        logger.info("Enhanced ETL job completed successfully.")

    except Exception as e:
        logger.error(f"ETL job failed with exception: {e}")
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped.")
```

### 2. Import Statement Updates
```python
# Add to existing imports
from pyspark.sql.functions import col, count, sum, coalesce, lit
```

## Data Model Updates

### 1. Source Data Model Changes

#### New Source Table: BRANCH_OPERATIONAL_DETAILS
- **Location**: Oracle Database
- **Purpose**: Store branch operational metadata
- **Key Fields**:
  - `BRANCH_ID` (INT) - Primary Key, Foreign Key to BRANCH table
  - `REGION` (VARCHAR2(50)) - Branch region identifier
  - `MANAGER_NAME` (VARCHAR2(100)) - Branch manager name
  - `LAST_AUDIT_DATE` (DATE) - Date of last audit
  - `IS_ACTIVE` (CHAR(1)) - Active status flag

### 2. Target Data Model Changes

#### Enhanced Target Table: BRANCH_SUMMARY_REPORT
- **Location**: Databricks Delta Lake
- **Changes**: Addition of two new columns

**New Columns Added:**
| Column Name | Data Type | Description | Source |
|-------------|-----------|-------------|--------|
| REGION | STRING | Branch region identifier | BRANCH_OPERATIONAL_DETAILS.REGION |
| LAST_AUDIT_DATE | STRING | Last audit date | BRANCH_OPERATIONAL_DETAILS.LAST_AUDIT_DATE |

**Updated Schema:**
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
```

### 3. Data Relationships

```
BRANCH (1) ←→ (1) BRANCH_OPERATIONAL_DETAILS
    ↓
ACCOUNT (N)
    ↓
TRANSACTION (N)
    ↓
BRANCH_SUMMARY_REPORT (Aggregated)
```

## Source-to-Target Mapping

### 1. Direct Field Mappings

| Source Table | Source Column | Target Table | Target Column | Transformation Rule |
|--------------|---------------|--------------|---------------|--------------------|
| BRANCH_OPERATIONAL_DETAILS | REGION | BRANCH_SUMMARY_REPORT | REGION | Direct mapping for active branches (IS_ACTIVE = 'Y') |
| BRANCH_OPERATIONAL_DETAILS | LAST_AUDIT_DATE | BRANCH_SUMMARY_REPORT | LAST_AUDIT_DATE | Convert DATE to STRING format |
| BRANCH_OPERATIONAL_DETAILS | BRANCH_ID | BRANCH_SUMMARY_REPORT | BRANCH_ID | Join key - no direct mapping |

### 2. Transformation Rules

#### 2.1 Active Branch Filter
```sql
WHERE BRANCH_OPERATIONAL_DETAILS.IS_ACTIVE = 'Y'
```

#### 2.2 Date Format Conversion
```python
# Convert Oracle DATE to STRING format
from pyspark.sql.functions import date_format

branch_operational_df = branch_operational_df.withColumn(
    "LAST_AUDIT_DATE", 
    date_format(col("LAST_AUDIT_DATE"), "yyyy-MM-dd")
)
```

#### 2.3 Left Join Logic
```python
# Maintain all branches even if operational details are missing
enhanced_summary = base_summary.join(
    branch_operational_df.select("BRANCH_ID", "REGION", "LAST_AUDIT_DATE"), 
    "BRANCH_ID", 
    "left"  # Left join to preserve all branches
)
```

### 3. Data Quality Rules

| Rule | Description | Implementation |
|------|-------------|----------------|
| Null Handling | Handle missing operational details gracefully | Use LEFT JOIN to preserve all branches |
| Active Status Filter | Only include active branches | Filter IS_ACTIVE = 'Y' before join |
| Date Validation | Ensure valid date formats | Apply date_format function |
| Branch ID Validation | Ensure referential integrity | Validate BRANCH_ID exists in BRANCH table |

## Assumptions and Constraints

### Assumptions
1. **Data Availability**: `BRANCH_OPERATIONAL_DETAILS` table is populated and accessible via existing Oracle JDBC connection
2. **Referential Integrity**: All `BRANCH_ID` values in `BRANCH_OPERATIONAL_DETAILS` exist in the `BRANCH` table
3. **Active Status**: Only branches with `IS_ACTIVE = 'Y'` should contribute operational metadata
4. **Backward Compatibility**: Existing records in `BRANCH_SUMMARY_REPORT` without operational details should remain valid
5. **Data Refresh**: Full reload of `BRANCH_SUMMARY_REPORT` is acceptable for this enhancement

### Constraints
1. **Performance**: Additional join operation may impact ETL performance
2. **Storage**: New columns will increase Delta table storage requirements
3. **Schema Evolution**: Delta table schema changes require careful deployment
4. **Data Governance**: New data elements must comply with existing data governance policies
5. **Security**: Oracle connection credentials must be managed securely

### Technical Constraints
1. **PySpark Version**: Code assumes PySpark 3.x compatibility
2. **Delta Lake**: Requires Delta Lake support in Databricks environment
3. **JDBC Driver**: Oracle JDBC driver must be available in Spark classpath
4. **Memory**: Additional data loading may require memory optimization

## References

### JIRA Stories
- **PCE-1**: PySpark Code enhancement for Databricks (Epic)
- **PCE-2**: Extend BRANCH_SUMMARY_REPORT Logic to Integrate New Source Table (Story)

### Source Files
- `Input/RegulatoryReportingETL.py` - Current ETL implementation
- `Input/Source_DDL.txt` - Source table schemas including new BRANCH_OPERATIONAL_DETAILS
- `Input/Target_DDL.txt` - Target table schema for BRANCH_SUMMARY_REPORT
- `Input/confluence_content.txt` - Business context and requirements
- `Input/branch_operational_details.sql` - Oracle DDL for new source table

### Technical Documentation
- PySpark SQL Functions Documentation
- Databricks Delta Lake Documentation
- Oracle JDBC Driver Documentation

---

**Document Version**: 1.0  
**Last Updated**: Generated automatically  
**Review Status**: Pending Technical Review