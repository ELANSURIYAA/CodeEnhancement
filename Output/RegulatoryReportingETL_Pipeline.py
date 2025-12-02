====================================================================
Author: Ascendion AAVA
Date: 
Description: Enhanced PySpark ETL integrating BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT with conditional logic and sample data for Databricks Delta.
====================================================================

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum, when

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_spark_session(app_name: str = "RegulatoryReportingETL") -> SparkSession:
    """
    Initializes and returns a Spark session compatible with Spark Connect.
    """
    try:
        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = SparkSession.builder.appName(app_name).getOrCreate()
        logger.info("Spark session created successfully.")
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark session: {e}")
        raise

# [ADDED] Sample data creation functions for self-contained ETL
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from datetime import date

def create_sample_dataframes(spark: SparkSession):
    """
    Creates sample DataFrames for all source tables.
    """
    customer_schema = StructType([
        StructField("CUSTOMER_ID", IntegerType(), True),
        StructField("NAME", StringType(), True),
        StructField("EMAIL", StringType(), True),
        StructField("PHONE", StringType(), True),
        StructField("ADDRESS", StringType(), True),
        StructField("CREATED_DATE", DateType(), True)
    ])
    customer_data = [
        (1, "Alice", "alice@example.com", "1234567890", "123 Main St", date(2023,1,1)),
        (2, "Bob", "bob@example.com", "2345678901", "456 Elm St", date(2023,2,1)),
    ]
    customer_df = spark.createDataFrame(customer_data, schema=customer_schema)

    branch_schema = StructType([
        StructField("BRANCH_ID", IntegerType(), True),
        StructField("BRANCH_NAME", StringType(), True),
        StructField("BRANCH_CODE", StringType(), True),
        StructField("CITY", StringType(), True),
        StructField("STATE", StringType(), True),
        StructField("COUNTRY", StringType(), True)
    ])
    branch_data = [
        (101, "Downtown", "DT001", "Metropolis", "NY", "USA"),
        (102, "Uptown", "UT002", "Metropolis", "NY", "USA"),
    ]
    branch_df = spark.createDataFrame(branch_data, schema=branch_schema)

    account_schema = StructType([
        StructField("ACCOUNT_ID", IntegerType(), True),
        StructField("CUSTOMER_ID", IntegerType(), True),
        StructField("BRANCH_ID", IntegerType(), True),
        StructField("ACCOUNT_NUMBER", StringType(), True),
        StructField("ACCOUNT_TYPE", StringType(), True),
        StructField("BALANCE", DoubleType(), True),
        StructField("OPENED_DATE", DateType(), True)
    ])
    account_data = [
        (1001, 1, 101, "ACC1001", "Savings", 5000.0, date(2023,3,1)),
        (1002, 2, 102, "ACC1002", "Checking", 12000.0, date(2023,4,1)),
    ]
    account_df = spark.createDataFrame(account_data, schema=account_schema)

    transaction_schema = StructType([
        StructField("TRANSACTION_ID", IntegerType(), True),
        StructField("ACCOUNT_ID", IntegerType(), True),
        StructField("TRANSACTION_TYPE", StringType(), True),
        StructField("AMOUNT", DoubleType(), True),
        StructField("TRANSACTION_DATE", DateType(), True),
        StructField("DESCRIPTION", StringType(), True)
    ])
    transaction_data = [
        (5001, 1001, "Deposit", 2000.0, date(2023,5,1), "Initial deposit"),
        (5002, 1002, "Withdrawal", 1500.0, date(2023,5,2), "ATM withdrawal"),
    ]
    transaction_df = spark.createDataFrame(transaction_data, schema=transaction_schema)

    branch_operational_schema = StructType([
        StructField("BRANCH_ID", IntegerType(), True),
        StructField("REGION", StringType(), True),
        StructField("MANAGER_NAME", StringType(), True),
        StructField("LAST_AUDIT_DATE", StringType(), True),  # Use string for compatibility
        StructField("IS_ACTIVE", StringType(), True)
    ])
    branch_operational_data = [
        (101, "East", "John Doe", "2023-06-01", "Y"),
        (102, "West", "Jane Smith", "2023-06-15", "N"),
    ]
    branch_operational_df = spark.createDataFrame(branch_operational_data, schema=branch_operational_schema)

    return customer_df, branch_df, account_df, transaction_df, branch_operational_df

# [DEPRECATED] Old branch_summary_report logic (without operational details)
def create_branch_summary_report_v1(transaction_df: DataFrame, account_df: DataFrame, branch_df: DataFrame) -> DataFrame:
    """
    [DEPRECATED] Original logic before BRANCH_OPERATIONAL_DETAILS integration.
    """
    logger.info("Creating Branch Summary Report DataFrame (v1, deprecated).")
    return transaction_df.join(account_df, "ACCOUNT_ID") \
                         .join(branch_df, "BRANCH_ID") \
                         .groupBy("BRANCH_ID", "BRANCH_NAME") \
                         .agg(
                             count("*").alias("TOTAL_TRANSACTIONS"),
                             sum("AMOUNT").alias("TOTAL_AMOUNT")
                         )

# [MODIFIED] Enhanced branch_summary_report logic with operational details
def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, branch_df: DataFrame, branch_operational_df: DataFrame) -> DataFrame:
    """
    Creates the BRANCH_SUMMARY_REPORT DataFrame by aggregating transaction data at the branch level and integrating operational details conditionally.
    """
    logger.info("Creating Branch Summary Report DataFrame (v2, enhanced).")
    base_df = transaction_df.join(account_df, "ACCOUNT_ID") \
                           .join(branch_df, "BRANCH_ID") \
                           .groupBy("BRANCH_ID", "BRANCH_NAME") \
                           .agg(
                               count("*").alias("TOTAL_TRANSACTIONS"),
                               sum("AMOUNT").alias("TOTAL_AMOUNT")
                           )
    # [ADDED] Join with BRANCH_OPERATIONAL_DETAILS
    result_df = base_df.join(branch_operational_df, "BRANCH_ID", "left")
    # [ADDED] Populate REGION and LAST_AUDIT_DATE only if IS_ACTIVE == 'Y'
    result_df = result_df.withColumn(
        "REGION", when(col("IS_ACTIVE") == "Y", col("REGION")).otherwise(None)
    ).withColumn(
        "LAST_AUDIT_DATE", when(col("IS_ACTIVE") == "Y", col("LAST_AUDIT_DATE")).otherwise(None)
    )
    # [ADDED] Select only required columns for target table
    result_df = result_df.select(
        "BRANCH_ID", "BRANCH_NAME", "TOTAL_TRANSACTIONS", "TOTAL_AMOUNT", "REGION", "LAST_AUDIT_DATE"
    )
    return result_df

# [MODIFIED] Write to delta table (simulated)
def write_to_delta_table(df: DataFrame, table_name: str):
    """
    [MODIFIED] Simulated Delta write - show output for testing.
    """
    logger.info(f"[SIMULATED] Writing DataFrame to Delta table: {table_name}")
    df.show(truncate=False)
    # In real scenario: df.write.format("delta").mode("overwrite").saveAsTable(table_name)

# [ADDED] Main ETL execution with sample data
def main():
    """
    Main ETL execution function (self-contained for Databricks).
    """
    spark = None
    try:
        spark = get_spark_session()
        customer_df, branch_df, account_df, transaction_df, branch_operational_df = create_sample_dataframes(spark)
        # [MODIFIED] Generate enhanced branch summary report
        branch_summary_df = create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_df)
        write_to_delta_table(branch_summary_df, "BRANCH_SUMMARY_REPORT")
        logger.info("ETL job completed successfully.")
    except Exception as e:
        logger.error(f"ETL job failed with exception: {e}")
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped.")

if __name__ == "__main__":
    main()

# =========================
# Summary of Updates
# =========================
# - Integrated BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT logic.
# - REGION and LAST_AUDIT_DATE columns populated only if IS_ACTIVE = 'Y'.
# - Old logic commented out and marked as [DEPRECATED].
# - All changes annotated and documented inline.
# - Sample data provided for self-contained execution.
# - Delta write is simulated for demonstration purposes.
