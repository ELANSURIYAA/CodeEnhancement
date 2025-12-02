====================================================================
Author: Ascendion AAVA
Date: 
Description: PySpark ETL for BRANCH_SUMMARY_REPORT with integration of BRANCH_OPERATIONAL_DETAILS and conditional population of REGION and LAST_AUDIT_DATE
====================================================================

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum, when

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# [MODIFIED] Use Spark Connect compatible session

def get_spark_session(app_name: str = "RegulatoryReportingETL") -> SparkSession:
    """
    Initializes and returns a Spark session (Spark Connect compatible).
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

# [ADDED] Sample Data Creation Functions

def create_sample_data(spark: SparkSession):
    """
    Creates sample DataFrames for CUSTOMER, ACCOUNT, TRANSACTION, BRANCH, BRANCH_OPERATIONAL_DETAILS.
    """
    customer_data = [
        (1, "Alice", "alice@example.com", "1234567890", "123 Main St", "2024-01-01"),
        (2, "Bob", "bob@example.com", "2345678901", "234 Oak St", "2024-01-02")
    ]
    customer_df = spark.createDataFrame(customer_data, ["CUSTOMER_ID", "NAME", "EMAIL", "PHONE", "ADDRESS", "CREATED_DATE"])

    account_data = [
        (101, 1, 10, "ACC001", "SAVINGS", 1000.0, "2024-01-05"),
        (102, 2, 20, "ACC002", "CHECKING", 2000.0, "2024-01-06")
    ]
    account_df = spark.createDataFrame(account_data, ["ACCOUNT_ID", "CUSTOMER_ID", "BRANCH_ID", "ACCOUNT_NUMBER", "ACCOUNT_TYPE", "BALANCE", "OPENED_DATE"])

    transaction_data = [
        (1001, 101, "DEPOSIT", 500.0, "2024-02-01", "Initial Deposit"),
        (1002, 102, "WITHDRAWAL", 300.0, "2024-02-02", "ATM Withdrawal")
    ]
    transaction_df = spark.createDataFrame(transaction_data, ["TRANSACTION_ID", "ACCOUNT_ID", "TRANSACTION_TYPE", "AMOUNT", "TRANSACTION_DATE", "DESCRIPTION"])

    branch_data = [
        (10, "Central", "CEN001", "New York", "NY", "USA"),
        (20, "West", "WST001", "San Francisco", "CA", "USA")
    ]
    branch_df = spark.createDataFrame(branch_data, ["BRANCH_ID", "BRANCH_NAME", "BRANCH_CODE", "CITY", "STATE", "COUNTRY"])

    branch_op_data = [
        (10, "East", "John Doe", "2024-03-01", "Y"),
        (20, "West", "Jane Smith", "2024-03-02", "N")
    ]
    branch_op_df = spark.createDataFrame(branch_op_data, ["BRANCH_ID", "REGION", "MANAGER_NAME", "LAST_AUDIT_DATE", "IS_ACTIVE"])

    return customer_df, account_df, transaction_df, branch_df, branch_op_df

# [DEPRECATED] Legacy Branch Summary Report logic (without operational details)
# def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, branch_df: DataFrame) -> DataFrame:
#     """
#     [DEPRECATED] Old logic without BRANCH_OPERATIONAL_DETAILS
#     """
#     logger.info("Creating Branch Summary Report DataFrame [DEPRECATED].")
#     return transaction_df.join(account_df, "ACCOUNT_ID") \
#                          .join(branch_df, "BRANCH_ID") \
#                          .groupBy("BRANCH_ID", "BRANCH_NAME") \
#                          .agg(
#                              count("*").alias("TOTAL_TRANSACTIONS"),
#                              sum("AMOUNT").alias("TOTAL_AMOUNT")
#                          )

# [MODIFIED] Enhanced Branch Summary Report logic with operational details

def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, branch_df: DataFrame, branch_op_df: DataFrame) -> DataFrame:
    """
    Creates the BRANCH_SUMMARY_REPORT DataFrame with integration of BRANCH_OPERATIONAL_DETAILS.
    Conditional population of REGION and LAST_AUDIT_DATE if IS_ACTIVE = 'Y'.
    """
    logger.info("Creating Enhanced Branch Summary Report DataFrame [MODIFIED].")
    summary_df = transaction_df.join(account_df, "ACCOUNT_ID") \
                              .join(branch_df, "BRANCH_ID") \
                              .groupBy("BRANCH_ID", "BRANCH_NAME") \
                              .agg(
                                  count("*").alias("TOTAL_TRANSACTIONS"),
                                  sum("AMOUNT").alias("TOTAL_AMOUNT")
                              )
    # [ADDED] Join with BRANCH_OPERATIONAL_DETAILS and conditional population
    summary_df = summary_df.join(branch_op_df, "BRANCH_ID", "left")
    summary_df = summary_df.withColumn(
        "REGION", when(col("IS_ACTIVE") == "Y", col("REGION")).otherwise(None)
    ).withColumn(
        "LAST_AUDIT_DATE", when(col("IS_ACTIVE") == "Y", col("LAST_AUDIT_DATE")).otherwise(None)
    )
    # Select required columns for target table
    summary_df = summary_df.select(
        col("BRANCH_ID"),
        col("BRANCH_NAME"),
        col("TOTAL_TRANSACTIONS"),
        col("TOTAL_AMOUNT"),
        col("REGION"),
        col("LAST_AUDIT_DATE")
    )
    return summary_df

# [MODIFIED] Delta table write logic (simulate only)

def write_to_delta_table(df: DataFrame, table_name: str):
    """
    Simulates writing a DataFrame to a Delta table (no actual write in test mode).
    """
    logger.info(f"[SIMULATED] Writing DataFrame to Delta table: {table_name}")
    df.show()

# [ADDED] Main function for ETL execution

def main():
    """
    Main ETL execution function (uses sample data).
    """
    spark = None
    try:
        spark = get_spark_session()

        # [ADDED] Create sample data
        customer_df, account_df, transaction_df, branch_df, branch_op_df = create_sample_data(spark)

        # [MODIFIED] Enhanced Branch Summary Report
        branch_summary_df = create_branch_summary_report(transaction_df, account_df, branch_df, branch_op_df)
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

# [SUMMARY OF UPDATION]
# - Integrated BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT logic
# - Added conditional population for REGION and LAST_AUDIT_DATE based on IS_ACTIVE
# - Deprecated legacy aggregation logic (commented out)
# - Updated code to be Spark Connect compatible
# - Provided sample data creation for self-contained ETL
# - All changes annotated inline with [ADDED], [MODIFIED], [DEPRECATED] tags
