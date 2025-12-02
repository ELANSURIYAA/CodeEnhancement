====================================================================
Author: Ascendion AAVA
Date: 
Description: Enhanced PySpark ETL integrating BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT with conditional population and audit-ready logic.
====================================================================

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum, when

# [ADDED] Configure logging for audit and traceability
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# [MODIFIED] Use Spark Connect compatible session

def get_spark_session(app_name: str = "RegulatoryReportingETL") -> SparkSession:
    """
    Initializes and returns a Spark session compatible with Spark Connect.
    """
    try:
        # [MODIFIED] Use getActiveSession for Databricks compatibility
        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = SparkSession.builder.appName(app_name).getOrCreate()
        logger.info("Spark session created successfully.")
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark session: {e}")
        raise

# [ADDED] Simulated Delta Table Reader

def read_delta_table(spark: SparkSession, table_name: str) -> DataFrame:
    """
    Reads a Delta table into a DataFrame. (Simulated for sample data)
    """
    logger.info(f"Reading Delta table: {table_name}")
    # [ADDED] For self-contained sample, return empty DataFrame
    return spark.createDataFrame([], schema=None)

# [ADDED] Sample Data Creation Functions

def create_sample_dataframes(spark: SparkSession):
    """
    Creates sample DataFrames for CUSTOMER, ACCOUNT, TRANSACTION, BRANCH, BRANCH_OPERATIONAL_DETAILS.
    """
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, DateType
    import datetime
    # CUSTOMER
    customer_schema = StructType([
        StructField("CUSTOMER_ID", IntegerType()),
        StructField("NAME", StringType()),
        StructField("EMAIL", StringType()),
        StructField("PHONE", StringType()),
        StructField("ADDRESS", StringType()),
        StructField("CREATED_DATE", DateType())
    ])
    customer_data = [
        (1, "Alice", "alice@example.com", "1234567890", "123 Main St", datetime.date(2023,1,1)),
        (2, "Bob", "bob@example.com", "0987654321", "456 Elm St", datetime.date(2023,2,1))
    ]
    customer_df = spark.createDataFrame(customer_data, schema=customer_schema)

    # ACCOUNT
    account_schema = StructType([
        StructField("ACCOUNT_ID", IntegerType()),
        StructField("CUSTOMER_ID", IntegerType()),
        StructField("BRANCH_ID", IntegerType()),
        StructField("ACCOUNT_NUMBER", StringType()),
        StructField("ACCOUNT_TYPE", StringType()),
        StructField("BALANCE", DecimalType(15,2)),
        StructField("OPENED_DATE", DateType())
    ])
    account_data = [
        (100, 1, 10, "A100", "SAVINGS", 1000.00, datetime.date(2023,1,5)),
        (101, 2, 11, "A101", "CHECKING", 2000.00, datetime.date(2023,2,5))
    ]
    account_df = spark.createDataFrame(account_data, schema=account_schema)

    # TRANSACTION
    transaction_schema = StructType([
        StructField("TRANSACTION_ID", IntegerType()),
        StructField("ACCOUNT_ID", IntegerType()),
        StructField("TRANSACTION_TYPE", StringType()),
        StructField("AMOUNT", DecimalType(15,2)),
        StructField("TRANSACTION_DATE", DateType()),
        StructField("DESCRIPTION", StringType())
    ])
    transaction_data = [
        (1000, 100, "DEPOSIT", 500.00, datetime.date(2023,1,10), "Salary"),
        (1001, 101, "WITHDRAWAL", 300.00, datetime.date(2023,2,10), "Bill Payment")
    ]
    transaction_df = spark.createDataFrame(transaction_data, schema=transaction_schema)

    # BRANCH
    branch_schema = StructType([
        StructField("BRANCH_ID", IntegerType()),
        StructField("BRANCH_NAME", StringType()),
        StructField("BRANCH_CODE", StringType()),
        StructField("CITY", StringType()),
        StructField("STATE", StringType()),
        StructField("COUNTRY", StringType())
    ])
    branch_data = [
        (10, "Central", "CEN", "Metropolis", "State1", "CountryA"),
        (11, "West", "WST", "Coast City", "State2", "CountryB")
    ]
    branch_df = spark.createDataFrame(branch_data, schema=branch_schema)

    # [ADDED] BRANCH_OPERATIONAL_DETAILS
    branch_operational_schema = StructType([
        StructField("BRANCH_ID", IntegerType()),
        StructField("REGION", StringType()),
        StructField("MANAGER_NAME", StringType()),
        StructField("LAST_AUDIT_DATE", StringType()),
        StructField("IS_ACTIVE", StringType())
    ])
    branch_operational_data = [
        (10, "North", "John Doe", "2023-03-01", "Y"),
        (11, "South", "Jane Smith", "2023-03-15", "N")
    ]
    branch_operational_df = spark.createDataFrame(branch_operational_data, schema=branch_operational_schema)

    return customer_df, account_df, transaction_df, branch_df, branch_operational_df

# [MODIFIED] Enhanced Branch Summary Report Logic

def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, branch_df: DataFrame, branch_operational_df: DataFrame) -> DataFrame:
    """
    Creates the BRANCH_SUMMARY_REPORT DataFrame by aggregating transaction data at the branch level and integrating operational details.
    """
    logger.info("Creating Branch Summary Report DataFrame with operational details integration.")
    summary_df = transaction_df.join(account_df, "ACCOUNT_ID") \
        .join(branch_df, "BRANCH_ID") \
        .groupBy("BRANCH_ID", "BRANCH_NAME") \
        .agg(
            count("*").alias("TOTAL_TRANSACTIONS"),
            sum("AMOUNT").alias("TOTAL_AMOUNT")
        )
    # [ADDED] Integrate BRANCH_OPERATIONAL_DETAILS
    summary_df = summary_df.join(branch_operational_df, "BRANCH_ID", "left")
    summary_df = summary_df.withColumn(
        "REGION", when(col("IS_ACTIVE") == "Y", col("REGION")).otherwise(None)
    ).withColumn(
        "LAST_AUDIT_DATE", when(col("IS_ACTIVE") == "Y", col("LAST_AUDIT_DATE")).otherwise(None)
    )
    # [MODIFIED] Select required columns for target Delta table
    summary_df = summary_df.select(
        "BRANCH_ID", "BRANCH_NAME", "TOTAL_TRANSACTIONS", "TOTAL_AMOUNT", "REGION", "LAST_AUDIT_DATE"
    )
    return summary_df

# [MODIFIED] Deprecated logic commented out for audit
# def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, branch_df: DataFrame) -> DataFrame:
#     """
#     [DEPRECATED] Old logic: Did not integrate operational details
#     """
#     logger.info("[DEPRECATED] Creating Branch Summary Report DataFrame without operational details.")
#     return transaction_df.join(account_df, "ACCOUNT_ID") \
#         .join(branch_df, "BRANCH_ID") \
#         .groupBy("BRANCH_ID", "BRANCH_NAME") \
#         .agg(
#             count("*").alias("TOTAL_TRANSACTIONS"),
#             sum("AMOUNT").alias("TOTAL_AMOUNT")
#         )

# [ADDED] Delta Table Writer (simulated for sample data)

def write_to_delta_table(df: DataFrame, table_name: str):
    """
    Simulates writing a DataFrame to a Delta table (prints schema and sample rows).
    """
    logger.info(f"Writing DataFrame to Delta table: {table_name}")
    df.show(truncate=False)
    logger.info(f"Successfully written data to {table_name}")

# [MODIFIED] Main ETL execution function

def main():
    """
    Main ETL execution function for Regulatory Reporting.
    """
    spark = None
    try:
        spark = get_spark_session()
        # [ADDED] Use sample data for self-contained pipeline
        customer_df, account_df, transaction_df, branch_df, branch_operational_df = create_sample_dataframes(spark)
        # [MODIFIED] Create Branch Summary Report with operational details
        branch_summary_df = create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_df)
        # [ADDED] Simulate writing to Delta table
        write_to_delta_table(branch_summary_df, "workspace.default.branch_summary_report")
        logger.info("ETL job completed successfully.")
    except Exception as e:
        logger.error(f"ETL job failed with exception: {e}")
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped.")

if __name__ == "__main__":
    main()

# ====================================================================
# Summary of Updation in this Version
# - Integrated BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT ETL logic
# - Added conditional population of REGION and LAST_AUDIT_DATE based on IS_ACTIVE
# - Deprecated old summary report logic (commented out for audit)
# - Enhanced documentation and inline comments for traceability
# ====================================================================
