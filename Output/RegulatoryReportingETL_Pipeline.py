====================================================================
Author: Ascendion AAVA
Date: 
Description: PySpark ETL pipeline enhanced to join BRANCH_OPERATIONAL_DETAILS and conditionally populate REGION and LAST_AUDIT_DATE in BRANCH_SUMMARY_REPORT.
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
    Initializes and returns a Spark session using getActiveSession for Spark Connect compatibility.
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

# [MODIFIED] Use Delta format for all reads/writes (simulate with sample data)

def read_delta_table(spark: SparkSession, table_name: str) -> DataFrame:
    """
    Reads a Delta table into a DataFrame. (Here, simulated with sample data)
    """
    logger.info(f"Reading Delta table: {table_name}")
    # [ADDED] For demonstration, return sample DataFrames
    if table_name == "CUSTOMER":
        return spark.createDataFrame([
            (1, "Alice", "alice@example.com", "1234567890", "123 Main St", "2023-01-01"),
            (2, "Bob", "bob@example.com", "0987654321", "456 Elm St", "2023-01-02")
        ], ["CUSTOMER_ID", "NAME", "EMAIL", "PHONE", "ADDRESS", "CREATED_DATE"])
    if table_name == "ACCOUNT":
        return spark.createDataFrame([
            (101, 1, 10, "ACCT001", "SAVINGS", 1000.00, "2023-01-01"),
            (102, 2, 20, "ACCT002", "CURRENT", 2000.00, "2023-01-02")
        ], ["ACCOUNT_ID", "CUSTOMER_ID", "BRANCH_ID", "ACCOUNT_NUMBER", "ACCOUNT_TYPE", "BALANCE", "OPENED_DATE"])
    if table_name == "TRANSACTION":
        return spark.createDataFrame([
            (1001, 101, "DEPOSIT", 500.00, "2023-02-01", "Salary"),
            (1002, 102, "WITHDRAWAL", 300.00, "2023-02-02", "Bill Payment")
        ], ["TRANSACTION_ID", "ACCOUNT_ID", "TRANSACTION_TYPE", "AMOUNT", "TRANSACTION_DATE", "DESCRIPTION"])
    if table_name == "BRANCH":
        return spark.createDataFrame([
            (10, "Central", "CEN001", "NYC", "NY", "USA"),
            (20, "West", "WST001", "LA", "CA", "USA")
        ], ["BRANCH_ID", "BRANCH_NAME", "BRANCH_CODE", "CITY", "STATE", "COUNTRY"])
    if table_name == "BRANCH_OPERATIONAL_DETAILS":
        return spark.createDataFrame([
            (10, "East", "John Doe", "2023-03-01", "Y"),
            (20, "West", "Jane Smith", "2023-03-05", "N")
        ], ["BRANCH_ID", "REGION", "MANAGER_NAME", "LAST_AUDIT_DATE", "IS_ACTIVE"])
    return spark.createDataFrame([], [])

# [MODIFIED] Enhanced branch summary logic

def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, branch_df: DataFrame, branch_operational_details_df: DataFrame) -> DataFrame:
    """
    Creates the BRANCH_SUMMARY_REPORT DataFrame by aggregating transaction data at the branch level and joining operational details.
    """
    logger.info("Creating Branch Summary Report DataFrame.")
    # [DEPRECATED] Old logic without operational details
    # deprecated_branch_summary = transaction_df.join(account_df, "ACCOUNT_ID") \
    #    .join(branch_df, "BRANCH_ID") \
    #    .groupBy("BRANCH_ID", "BRANCH_NAME") \
    #    .agg(count("*").alias("TOTAL_TRANSACTIONS"), sum("AMOUNT").alias("TOTAL_AMOUNT"))
    # [MODIFIED] New logic integrating BRANCH_OPERATIONAL_DETAILS
    branch_summary = transaction_df.join(account_df, "ACCOUNT_ID") \
        .join(branch_df, "BRANCH_ID") \
        .groupBy("BRANCH_ID", "BRANCH_NAME") \
        .agg(
            count("*").alias("TOTAL_TRANSACTIONS"),
            sum("AMOUNT").alias("TOTAL_AMOUNT")
        )
    branch_summary = branch_summary.join(branch_operational_details_df, "BRANCH_ID", "left")
    branch_summary = branch_summary.withColumn(
        "REGION", when(col("IS_ACTIVE") == "Y", col("REGION")).otherwise(None)
    ).withColumn(
        "LAST_AUDIT_DATE", when(col("IS_ACTIVE") == "Y", col("LAST_AUDIT_DATE")).otherwise(None)
    )
    # [ADDED] Select relevant columns for target schema
    return branch_summary.select(
        "BRANCH_ID", "BRANCH_NAME", "TOTAL_TRANSACTIONS", "TOTAL_AMOUNT", "REGION", "LAST_AUDIT_DATE"
    )

# [MODIFIED] Write to Delta table (simulated)
def write_to_delta_table(df: DataFrame, table_name: str):
    """
    Writes a DataFrame to a Delta table. (Here, just shows the output)
    """
    logger.info(f"Writing DataFrame to Delta table: {table_name}")
    df.show()
    # In Databricks, would use: df.write.format("delta").mode("overwrite").saveAsTable(table_name)

# [ADDED] Main ETL function with operational details integration
def main():
    """
    Main ETL execution function with enhanced branch summary logic.
    """
    spark = None
    try:
        spark = get_spark_session()
        # [MODIFIED] Read all source tables as Delta (sample data)
        customer_df = read_delta_table(spark, "CUSTOMER")
        account_df = read_delta_table(spark, "ACCOUNT")
        transaction_df = read_delta_table(spark, "TRANSACTION")
        branch_df = read_delta_table(spark, "BRANCH")
        branch_operational_details_df = read_delta_table(spark, "BRANCH_OPERATIONAL_DETAILS")
        # [MODIFIED] Enhanced branch summary ETL
        branch_summary_df = create_branch_summary_report(
            transaction_df, account_df, branch_df, branch_operational_details_df
        )
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
# - Integrated BRANCH_OPERATIONAL_DETAILS into branch summary ETL
# - Added conditional logic for REGION and LAST_AUDIT_DATE
# - Deprecated old branch summary logic (commented out)
# - Used sample data for demonstration, Delta table reads/writes simulated
