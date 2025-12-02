#====================================================================
# Author: Ascendion AAVA
# Date: 
# Description: Enhanced PySpark ETL for BRANCH_SUMMARY_REPORT with integration of BRANCH_OPERATIONAL_DETAILS
#====================================================================

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum, when
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# [ADDED] Use Spark Connect compatible session

def get_spark_session(app_name: str = "RegulatoryReportingETL") -> SparkSession:
    """
    Initializes and returns a Spark session.
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

# [ADDED] Sample data for testing (self-contained, no JDBC)
def get_sample_data(spark):
    """Create sample DataFrames for CUSTOMER, ACCOUNT, TRANSACTION, BRANCH, BRANCH_OPERATIONAL_DETAILS"""
    customer_schema = StructType([
        StructField("CUSTOMER_ID", IntegerType()),
        StructField("NAME", StringType()),
        StructField("EMAIL", StringType()),
        StructField("PHONE", StringType()),
        StructField("ADDRESS", StringType()),
        StructField("CREATED_DATE", StringType()),
    ])
    customer_data = [
        (1, "John Doe", "john@example.com", "1234567890", "123 Elm St", "2023-01-01"),
        (2, "Jane Smith", "jane@example.com", "2345678901", "456 Oak St", "2023-02-01"),
    ]
    customer_df = spark.createDataFrame(customer_data, schema=customer_schema)

    account_schema = StructType([
        StructField("ACCOUNT_ID", IntegerType()),
        StructField("CUSTOMER_ID", IntegerType()),
        StructField("BRANCH_ID", IntegerType()),
        StructField("ACCOUNT_NUMBER", StringType()),
        StructField("ACCOUNT_TYPE", StringType()),
        StructField("BALANCE", DoubleType()),
        StructField("OPENED_DATE", StringType()),
    ])
    account_data = [
        (101, 1, 10, "ACC1001", "SAVINGS", 1000.0, "2023-01-10"),
        (102, 2, 20, "ACC1002", "CHECKING", 2000.0, "2023-02-10"),
    ]
    account_df = spark.createDataFrame(account_data, schema=account_schema)

    transaction_schema = StructType([
        StructField("TRANSACTION_ID", IntegerType()),
        StructField("ACCOUNT_ID", IntegerType()),
        StructField("TRANSACTION_TYPE", StringType()),
        StructField("AMOUNT", DoubleType()),
        StructField("TRANSACTION_DATE", StringType()),
        StructField("DESCRIPTION", StringType()),
    ])
    transaction_data = [
        (1001, 101, "DEPOSIT", 500.0, "2023-03-01", "Initial Deposit"),
        (1002, 102, "WITHDRAWAL", 300.0, "2023-03-02", "ATM Withdrawal"),
    ]
    transaction_df = spark.createDataFrame(transaction_data, schema=transaction_schema)

    branch_schema = StructType([
        StructField("BRANCH_ID", IntegerType()),
        StructField("BRANCH_NAME", StringType()),
        StructField("BRANCH_CODE", StringType()),
        StructField("CITY", StringType()),
        StructField("STATE", StringType()),
        StructField("COUNTRY", StringType()),
    ])
    branch_data = [
        (10, "Downtown", "DT01", "Metropolis", "CA", "USA"),
        (20, "Uptown", "UT02", "Metropolis", "CA", "USA"),
    ]
    branch_df = spark.createDataFrame(branch_data, schema=branch_schema)

    bod_schema = StructType([
        StructField("BRANCH_ID", IntegerType()),
        StructField("REGION", StringType()),
        StructField("MANAGER_NAME", StringType()),
        StructField("LAST_AUDIT_DATE", StringType()),
        StructField("IS_ACTIVE", StringType()),
    ])
    bod_data = [
        (10, "North", "Alice", "2023-05-01", "Y"),
        (20, "South", "Bob", "2023-06-01", "N"),
    ]
    bod_df = spark.createDataFrame(bod_data, schema=bod_schema)

    return customer_df, account_df, transaction_df, branch_df, bod_df

# [UNCHANGED] AML_CUSTOMER_TRANSACTIONS logic
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

# [MODIFIED] BRANCH_SUMMARY_REPORT logic
# [DEPRECATED] Old logic without operational details:
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

# [ADDED] New logic with BRANCH_OPERATIONAL_DETAILS integration
def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, branch_df: DataFrame, bod_df: DataFrame) -> DataFrame:
    """
    Creates the BRANCH_SUMMARY_REPORT DataFrame by aggregating transaction data at the branch level
    and integrating BRANCH_OPERATIONAL_DETAILS for REGION and LAST_AUDIT_DATE columns.
    """
    logger.info("Creating Branch Summary Report DataFrame with Operational Details.")
    summary_df = transaction_df.join(account_df, "ACCOUNT_ID") \
                              .join(branch_df, "BRANCH_ID") \
                              .groupBy("BRANCH_ID", "BRANCH_NAME") \
                              .agg(
                                  count("*").alias("TOTAL_TRANSACTIONS"),
                                  sum("AMOUNT").alias("TOTAL_AMOUNT")
                              )
    # [ADDED] Join with operational details
    summary_df = summary_df.join(bod_df, "BRANCH_ID", "left")
    # [ADDED] Populate REGION and LAST_AUDIT_DATE only if IS_ACTIVE == 'Y'
    summary_df = summary_df.withColumn(
        "REGION", when(col("IS_ACTIVE") == "Y", col("REGION")).otherwise(None)
    ).withColumn(
        "LAST_AUDIT_DATE", when(col("IS_ACTIVE") == "Y", col("LAST_AUDIT_DATE")).otherwise(None)
    )
    return summary_df.select(
        "BRANCH_ID", "BRANCH_NAME", "TOTAL_TRANSACTIONS", "TOTAL_AMOUNT", "REGION", "LAST_AUDIT_DATE"
    )

# [ADDED] Write to Delta (simulated)
def write_to_delta_table(df: DataFrame, table_name: str):
    """
    Simulate writing a DataFrame to a Delta table (for local test, just show output).
    """
    logger.info(f"Writing DataFrame to Delta table: {table_name}")
    df.show(truncate=False)
    # In Databricks, use: df.write.format("delta").mode("overwrite").saveAsTable(table_name)

# [ADDED] Main function for self-contained pipeline execution
def main():
    spark = get_spark_session()
    customer_df, account_df, transaction_df, branch_df, bod_df = get_sample_data(spark)

    # Create and write AML_CUSTOMER_TRANSACTIONS
    aml_transactions_df = create_aml_customer_transactions(customer_df, account_df, transaction_df)
    write_to_delta_table(aml_transactions_df, "AML_CUSTOMER_TRANSACTIONS")

    # Create and write enhanced BRANCH_SUMMARY_REPORT
    branch_summary_df = create_branch_summary_report(transaction_df, account_df, branch_df, bod_df)
    write_to_delta_table(branch_summary_df, "BRANCH_SUMMARY_REPORT")

    logger.info("ETL job completed successfully.")

if __name__ == "__main__":
    main()
