=============================================
Author: Ascendion AAVA
Date: 
Description: Technical specification for integrating BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT ETL pipeline
=============================================

# Technical Specification for BRANCH_OPERATIONAL_DETAILS Integration Enhancement

## Introduction

### Project Overview
This technical specification outlines the enhancement required to integrate the new Oracle source table `BRANCH_OPERATIONAL_DETAILS` into the existing ETL pipeline that generates the `BRANCH_SUMMARY_REPORT` in Databricks Delta format. The enhancement aims to improve compliance and audit readiness by incorporating branch-level operational metadata.

### Business Requirements
Based on JIRA story PCE-2, the following business requirements have been identified:
- Integrate `BRANCH_OPERATIONAL_DETAILS` table containing branch operational metadata
- Add two new columns (`REGION` and `LAST_AUDIT_DATE`) to `BRANCH_SUMMARY_REPORT`
- Maintain backward compatibility with existing records
- Ensure data is populated conditionally based on `IS_ACTIVE = 'Y'`

### Scope
This specification covers:
- Code changes to the existing PySpark ETL pipeline (`RegulatoryReportingETL.py`)
- Data model updates for both source and target structures
- Source-to-target field mapping with transformation rules
- Impact analysis on existing functionality

## Code Changes

### 1. Main ETL Function Updates

#### Current Implementation Analysis
The existing `main()` function in `RegulatoryReportingETL.py` reads four source tables:
- CUSTOMER
- ACCOUNT
- TRANSACTION
- BRANCH

#### Required Changes

**1.1 Add New Source Table Reading**
```python
# Add after existing table reads
branch_operational_df = read_table(spark, jdbc_url, "BRANCH_OPERATIONAL_DETAILS", connection_properties)
```

**1.2 Update Branch Summary Report Creation**
```python
# Replace existing branch summary creation
branch_summary_df = create_enhanced_branch_summary_report(
    transaction_df, 
    account_df, 
    branch_df, 
    branch_operational_df
)
```

### 2. New Function Implementation

#### 2.1 Enhanced Branch Summary Report Function
```python
def create_enhanced_branch_summary_report(
    transaction_df: DataFrame, 
    account_df: DataFrame, 
    branch_df: DataFrame,
    branch_operational_df: DataFrame
) -> DataFrame:
    """
    Creates the enhanced BRANCH_SUMMARY_REPORT DataFrame by aggregating transaction data 
    at the branch level and incorporating operational metadata.
    
    :param transaction_df: DataFrame with transaction data.
    :param account_df: DataFrame with account data.
    :param branch_df: DataFrame with branch data.
    :param branch_operational_df: DataFrame with branch operational details.
    :return: A DataFrame containing the enhanced branch summary report.
    """
    logger.info("Creating Enhanced Branch Summary Report DataFrame.")
    
    # Filter active branches only
    active_branch_operational_df = branch_operational_df.filter(
        col("IS_ACTIVE") == "Y"
    )
    
    # Create base summary as before
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
            "BRANCH_ID", 
            "REGION", 
            "LAST_AUDIT_DATE"
        ),
        "BRANCH_ID",
        "left"
    )
    
    return enhanced_summary
```

#### 2.2 Import Statement Updates
```python
# Add to existing imports
from pyspark.sql.functions import col, count, sum, when, lit
```

### 3. Error Handling and Logging Enhancements

#### 3.1 Add Validation Function
```python
def validate_branch_operational_data(branch_operational_df: DataFrame) -> bool:
    """
    Validates the BRANCH_OPERATIONAL_DETAILS data quality.
    
    :param branch_operational_df: DataFrame to validate
    :return: Boolean indicating validation success
    """
    try:
        # Check for required columns
        required_columns = ["BRANCH_ID", "REGION", "IS_ACTIVE"]
        missing_columns = set(required_columns) - set(branch_operational_df.columns)
        
        if missing_columns:
            logger.error(f"Missing required columns: {missing_columns}")
            return False
            
        # Check for null BRANCH_IDs
        null_branch_ids = branch_operational_df.filter(col("BRANCH_ID").isNull()).count()
        if null_branch_ids > 0:
            logger.warning(f"Found {null_branch_ids} records with null BRANCH_ID")
            
        logger.info("Branch operational data validation completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Validation failed: {e}")
        return False
```

## Data Model Updates

### 1. Source Data Model Changes

#### 1.1 New Source Table: BRANCH_OPERATIONAL_DETAILS
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
- Primary key: BRANCH_ID
- Mandatory fields: REGION, IS_ACTIVE
- Optional fields: MANAGER_NAME, LAST_AUDIT_DATE
- Default value for IS_ACTIVE: 'Y'

#### 1.2 Existing Source Tables (No Changes)
- CUSTOMER: No modifications required
- ACCOUNT: No modifications required
- TRANSACTION: No modifications required
- BRANCH: No modifications required

### 2. Target Data Model Changes

#### 2.1 Enhanced BRANCH_SUMMARY_REPORT Schema
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
- Added `REGION STRING`: Nullable field to store branch region information
- Added `LAST_AUDIT_DATE STRING`: Nullable field to store last audit date

#### 2.2 Data Type Considerations
- `LAST_AUDIT_DATE` stored as STRING in target for flexibility and to handle null values
- `REGION` stored as STRING with no length restrictions in Delta format

## Source-to-Target Mapping

### 1. Field Mapping Table

| Source Table | Source Column | Target Table | Target Column | Transformation Rule | Data Type | Nullable |
|--------------|---------------|--------------|---------------|-------------------|-----------|----------|
| TRANSACTION | COUNT(*) | BRANCH_SUMMARY_REPORT | TOTAL_TRANSACTIONS | Aggregation by BRANCH_ID | BIGINT | No |
| TRANSACTION | SUM(AMOUNT) | BRANCH_SUMMARY_REPORT | TOTAL_AMOUNT | Aggregation by BRANCH_ID | DOUBLE | No |
| BRANCH | BRANCH_ID | BRANCH_SUMMARY_REPORT | BRANCH_ID | Direct mapping | BIGINT | No |
| BRANCH | BRANCH_NAME | BRANCH_SUMMARY_REPORT | BRANCH_NAME | Direct mapping | STRING | No |
| BRANCH_OPERATIONAL_DETAILS | REGION | BRANCH_SUMMARY_REPORT | REGION | Conditional mapping (IS_ACTIVE='Y') | STRING | Yes |
| BRANCH_OPERATIONAL_DETAILS | LAST_AUDIT_DATE | BRANCH_SUMMARY_REPORT | LAST_AUDIT_DATE | Conditional mapping + Date to String conversion | STRING | Yes |

### 2. Transformation Rules

#### 2.1 Region Mapping
```sql
CASE 
    WHEN BRANCH_OPERATIONAL_DETAILS.IS_ACTIVE = 'Y' 
    THEN BRANCH_OPERATIONAL_DETAILS.REGION 
    ELSE NULL 
END AS REGION
```

#### 2.2 Last Audit Date Mapping
```sql
CASE 
    WHEN BRANCH_OPERATIONAL_DETAILS.IS_ACTIVE = 'Y' 
    THEN CAST(BRANCH_OPERATIONAL_DETAILS.LAST_AUDIT_DATE AS STRING)
    ELSE NULL 
END AS LAST_AUDIT_DATE
```

#### 2.3 Join Logic
```sql
LEFT JOIN BRANCH_OPERATIONAL_DETAILS 
    ON BRANCH.BRANCH_ID = BRANCH_OPERATIONAL_DETAILS.BRANCH_ID 
    AND BRANCH_OPERATIONAL_DETAILS.IS_ACTIVE = 'Y'
```

### 3. Business Rules

#### 3.1 Data Population Rules
- Only active branches (IS_ACTIVE = 'Y') contribute operational metadata
- Inactive branches will have NULL values for REGION and LAST_AUDIT_DATE
- All existing functionality for TOTAL_TRANSACTIONS and TOTAL_AMOUNT remains unchanged

#### 3.2 Backward Compatibility
- Existing records without operational details will show NULL for new columns
- No impact on existing aggregation logic
- Historical data integrity maintained

## Assumptions and Constraints

### 1. Technical Assumptions
- Oracle JDBC driver is available in the Spark classpath
- BRANCH_OPERATIONAL_DETAILS table exists and is accessible via the same JDBC connection
- Databricks Delta table supports schema evolution for adding new columns
- Sufficient memory and compute resources for the enhanced join operation

### 2. Data Assumptions
- BRANCH_ID in BRANCH_OPERATIONAL_DETAILS corresponds to existing BRANCH_ID values
- IS_ACTIVE field contains only 'Y' or 'N' values
- REGION field contains valid region codes/names
- LAST_AUDIT_DATE follows standard date format

### 3. Business Constraints
- Full reload of BRANCH_SUMMARY_REPORT required for initial deployment
- Deployment must be coordinated with data validation routines
- Backward compatibility must be maintained for downstream consumers

### 4. Performance Considerations
- Additional join operation may impact ETL performance
- Consider indexing on BRANCH_ID in source Oracle table
- Monitor Delta table file sizes after schema changes

## References

### 1. JIRA Stories
- **PCE-1**: PySpark Code enhancement for Databricks (Epic)
- **PCE-2**: Extend BRANCH_SUMMARY_REPORT Logic to Integrate New Source Table (Story)

### 2. Source Files
- `RegulatoryReportingETL.py`: Existing PySpark ETL implementation
- `Source_DDL.txt`: Source table definitions including new BRANCH_OPERATIONAL_DETAILS
- `Target_DDL.txt`: Target Delta table schema
- `confluence_content.txt`: Business context and requirements
- `branch_operational_details.sql`: Detailed DDL for new source table

### 3. Technical Documentation
- Databricks Delta Lake documentation
- PySpark SQL functions reference
- Oracle JDBC driver configuration guide

### 4. Deployment Notes
- Requires full reload of BRANCH_SUMMARY_REPORT table
- Schema evolution must be enabled on Delta table
- Coordinate with downstream data consumers for new column availability
- Update data validation and reconciliation routines to include new fields

---

**Document Version**: 1.0  
**Last Updated**: Current Date  
**Review Status**: Draft  
**Approved By**: Pending Review