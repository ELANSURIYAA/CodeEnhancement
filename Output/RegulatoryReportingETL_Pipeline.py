====================================================================
Author: Ascendion AAVA
Date: 
Description: Enhanced PySpark ETL for BRANCH_SUMMARY_REPORT to integrate BRANCH_OPERATIONAL_DETAILS with conditional population of REGION and LAST_AUDIT_DATE.
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
        if not spark:
            spark = SparkSession.builder.appName(app_name).getOrCreate()
        logger.info("Spark session created successfully.")
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark session: {e}")
        raise

# [ADDED] Sample data creation for all source tables
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from datetime import date

# Sample CUSTOMER
customer_data = [
    (1, "Alice", "alice@email.com", "1234567890", "1 Main St", date(2021, 1, 1)),
    (2, "Bob", "bob@email.com", "2345678901", "2 Main St", date(2021, 2, 1)),
]
customer_schema = StructType([
    StructField("CUSTOMER_ID", IntegerType(), True),
    StructField("NAME", StringType(), True),
    StructField("EMAIL", StringType(), True),
    StructField("PHONE", StringType(), True),
    StructField("ADDRESS", StringType(), True),
    StructField("CREATED_DATE", DateType(), True),
])

# Sample BRANCH
branch_data = [
    (101, "Downtown", "DT001", "CityA", "StateA", "CountryA"),
    (102, "Uptown", "UT002", "CityB", "StateB", "CountryB"),
]
branch_schema = StructType([
    StructField("BRANCH_ID", IntegerType(), True),
    StructField("BRANCH_NAME", StringType(), True),
    StructField("BRANCH_CODE", StringType(), True),
    StructField("CITY", StringType(), True),
    StructField("STATE", StringType(), True),
    StructField("COUNTRY", StringType(), True),
])

# Sample ACCOUNT
account_data = [
    (1001, 1, 101, "ACC001", "Savings", 1000.00, date(2021, 1, 1)),
    (1002, 2, 102, "ACC002", "Checking", 2000.00, date(2021, 2, 1)),
]
account_schema = StructType([
    StructField("ACCOUNT_ID", IntegerType(), True),
    StructField("CUSTOMER_ID", IntegerType(), True),
    StructField("BRANCH_ID", IntegerType(), True),
    StructField("ACCOUNT_NUMBER", StringType(), True),
    StructField("ACCOUNT_TYPE", StringType(), True),
    StructField("BALANCE", DoubleType(), True),
    StructField("OPENED_DATE", DateType(), True),
])

# Sample TRANSACTION
transaction_data = [
    (5001, 1001, "Deposit", 500.0, date(2022, 1, 15), "Salary"),
    (5002, 1002, "Withdrawal", 300.0, date(2022, 2, 15), "ATM"),
]
transaction_schema = StructType([
    StructField("TRANSACTION_ID", IntegerType(), True),
    StructField("ACCOUNT_ID", IntegerType(), True),
    StructField("TRANSACTION_TYPE", StringType(), True),
    StructField("AMOUNT", DoubleType(), True),
    StructField("TRANSACTION_DATE", DateType(), True),
    StructField("DESCRIPTION", StringType(), True),
])

# Sample BRANCH_OPERATIONAL_DETAILS
branch_operational_details_data = [
    (101, "North", "John Doe", date(2022, 5, 10), "Y"),
    (102, "South", "Jane Smith", date(2022, 6, 20), "N"),
]
branch_operational_details_schema = StructType([
    StructField("BRANCH_ID", IntegerType(), True),
    StructField("REGION", StringType(), True),
    StructField("MANAGER_NAME", StringType(), True),
    StructField("LAST_AUDIT_DATE", DateType(), True),
    StructField("IS_ACTIVE", StringType(), True),
])

def create_aml_customer_transactions(customer_df: DataFrame, account_df: DataFrame, transaction_df: DataFrame) -> DataFrame:
    """
    Creates the AML_CUSTOMER_TRANSACTIONS DataFrame by joining customer, account, and transaction data.
    """
    logger.info("Creating AML Customer Transactions DataFrame.")
    return customer_df.join(account_df, "CUSTOMER_ID") \
                      .join(transaction_df, "ACCOUNT_ID") \
                      .select(
                          col("CUSTOMER_ID"),
                          col("NAME"),
                          col("ACCOUNT_ID"),
                          col("TRANSACTION_ID"),
                          col("AMOUNT"),
                          col("TRANSACTION_TYPE"),
                          col("TRANSACTION_DATE")
                      )

# [DEPRECATED] Old branch_summary_report logic without operational details
# def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, branch_df: DataFrame) -> DataFrame:
#     """
#     Creates the BRANCH_SUMMARY_REPORT DataFrame by aggregating transaction data at the branch level.
#     """
#     logger.info("Creating Branch Summary Report DataFrame.")
#     return transaction_df.join(account_df, "ACCOUNT_ID") \
#                          .join(branch_df, "BRANCH_ID") \
#                          .groupBy("BRANCH_ID", "BRANCH_NAME") \
#                          .agg(
#                              count("*").alias("TOTAL_TRANSACTIONS"),
#                              sum("AMOUNT").alias("TOTAL_AMOUNT")
#                          )

# [MODIFIED] New branch_summary_report logic integrating BRANCH_OPERATIONAL_DETAILS

def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, branch_df: DataFrame, branch_operational_details_df: DataFrame) -> DataFrame:
    """
    Creates the BRANCH_SUMMARY_REPORT DataFrame by aggregating transaction data at the branch level,
    and joins with BRANCH_OPERATIONAL_DETAILS for REGION and LAST_AUDIT_DATE (conditional on IS_ACTIVE = 'Y').
    """
    logger.info("Creating Branch Summary Report DataFrame with operational details.")
    base_summary = transaction_df.join(account_df, "ACCOUNT_ID") \
                                 .join(branch_df, "BRANCH_ID") \
                                 .groupBy("BRANCH_ID", "BRANCH_NAME") \
                                 .agg(
                                     count("*").alias("TOTAL_TRANSACTIONS"),
                                     sum("AMOUNT").alias("TOTAL_AMOUNT")
                                 )
    # [ADDED] Join with BRANCH_OPERATIONAL_DETAILS and populate REGION/LAST_AUDIT_DATE conditionally
    summary_with_ops = base_summary.join(branch_operational_details_df, "BRANCH_ID", "left") \
        .withColumn("REGION", when(col("IS_ACTIVE") == "Y", col("REGION")).otherwise(None)) \
        .withColumn("LAST_AUDIT_DATE", when(col("IS_ACTIVE") == "Y", col("LAST_AUDIT_DATE")).otherwise(None))
    return summary_with_ops.select(
        "BRANCH_ID", "BRANCH_NAME", "TOTAL_TRANSACTIONS", "TOTAL_AMOUNT", "REGION", "LAST_AUDIT_DATE"
    )

def write_to_delta_table(df: DataFrame, table_name: str):
    """
    Writes a DataFrame to a Delta table (simulated by showing .show()).
    """
    logger.info(f"Writing DataFrame to Delta table: {table_name}")
    try:
        # For demo, just show the DataFrame
        df.show(truncate=False)
        logger.info(f"Successfully written data to {table_name}")
    except Exception as e:
        logger.error(f"Failed to write to Delta table {table_name}: {e}")
        raise

def main():
    """
    Main ETL execution function.
    """
    spark = None
    try:
        spark = get_spark_session()

        # [ADDED] Create DataFrames from sample data
        customer_df = spark.createDataFrame(customer_data, schema=customer_schema)
        account_df = spark.createDataFrame(account_data, schema=account_schema)
        transaction_df = spark.createDataFrame(transaction_data, schema=transaction_schema)
        branch_df = spark.createDataFrame(branch_data, schema=branch_schema)
        branch_operational_details_df = spark.createDataFrame(branch_operational_details_data, schema=branch_operational_details_schema)

        # Create and write AML_CUSTOMER_TRANSACTIONS
        aml_transactions_df = create_aml_customer_transactions(customer_df, account_df, transaction_df)
        write_to_delta_table(aml_transactions_df, "AML_CUSTOMER_TRANSACTIONS")

        # Create and write BRANCH_SUMMARY_REPORT
        branch_summary_df = create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_details_df)
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
# - Integrated BRANCH_OPERATIONAL_DETAILS source table into BRANCH_SUMMARY_REPORT ETL logic
# - Conditional population of REGION and LAST_AUDIT_DATE columns based on IS_ACTIVE = 'Y'
# - Deprecated old branch_summary_report logic (commented out)
# - Added sample data for all tables for self-contained execution
# - Ensured all changes are annotated with [ADDED], [MODIFIED], [DEPRECATED] tags
