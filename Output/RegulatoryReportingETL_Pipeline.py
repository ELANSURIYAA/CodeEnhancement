# ====================================================================
# Author: Ascendion AAVA
# Date: 
# Description: Enhanced PySpark ETL for BRANCH_SUMMARY_REPORT integrating BRANCH_OPERATIONAL_DETAILS with conditional columns REGION and LAST_AUDIT_DATE
# ====================================================================

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum, when
from delta.tables import DeltaTable

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_spark_session(app_name: str = "RegulatoryReportingETL") -> SparkSession:
    """
    Initializes and returns a Spark session compatible with Databricks and Spark Connect.
    """
    try:
        # [MODIFIED] Use getActiveSession for Spark Connect compatibility
        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = SparkSession.builder.appName(app_name).getOrCreate()
        logger.info("Spark session created successfully.")
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark session: {e}")
        raise

def create_sample_data(spark: SparkSession):
    """
    [ADDED] Creates sample DataFrames to simulate source tables.
    """
    # CUSTOMER table
    customer_df = spark.createDataFrame([
        (1, "Alice", "alice@example.com", "1234567890", "Addr1", "2023-01-01"),
        (2, "Bob", "bob@example.com", "0987654321", "Addr2", "2023-01-02")
    ], ["CUSTOMER_ID", "NAME", "EMAIL", "PHONE", "ADDRESS", "CREATED_DATE"])

    # BRANCH table
    branch_df = spark.createDataFrame([
        (100, "Central", "CEN", "City1", "State1", "Country1"),
        (200, "West", "WES", "City2", "State2", "Country2")
    ], ["BRANCH_ID", "BRANCH_NAME", "BRANCH_CODE", "CITY", "STATE", "COUNTRY"])

    # ACCOUNT table
    account_df = spark.createDataFrame([
        (1000, 1, 100, "ACC1000", "SAVINGS", 5000.0, "2023-01-05"),
        (1001, 2, 200, "ACC1001", "CHECKING", 10000.0, "2023-01-06")
    ], ["ACCOUNT_ID", "CUSTOMER_ID", "BRANCH_ID", "ACCOUNT_NUMBER", "ACCOUNT_TYPE", "BALANCE", "OPENED_DATE"])

    # TRANSACTION table
    transaction_df = spark.createDataFrame([
        (5001, 1000, "DEPOSIT", 1000.0, "2023-02-01", "Initial deposit"),
        (5002, 1001, "WITHDRAWAL", 2000.0, "2023-02-02", "ATM withdrawal")
    ], ["TRANSACTION_ID", "ACCOUNT_ID", "TRANSACTION_TYPE", "AMOUNT", "TRANSACTION_DATE", "DESCRIPTION"])

    # BRANCH_OPERATIONAL_DETAILS table
    branch_operational_details_df = spark.createDataFrame([
        (100, "North Region", "John Doe", "2023-01-10", "Y"),
        (200, "West Region", "Jane Smith", "2023-01-12", "N")
    ], ["BRANCH_ID", "REGION", "MANAGER_NAME", "LAST_AUDIT_DATE", "IS_ACTIVE"])

    return customer_df, account_df, transaction_df, branch_df, branch_operational_details_df

def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, branch_df: DataFrame, branch_operational_details_df: DataFrame) -> DataFrame:
    """
    [MODIFIED] Creates the BRANCH_SUMMARY_REPORT DataFrame by aggregating transaction data at the branch level
    and joining with BRANCH_OPERATIONAL_DETAILS to populate REGION and LAST_AUDIT_DATE conditionally.
    """
    logger.info("Creating Branch Summary Report DataFrame with operational details.")
    # Old logic (commented out for traceability)
    # branch_summary = transaction_df.join(account_df, "ACCOUNT_ID") \
    #     .join(branch_df, "BRANCH_ID") \
    #     .groupBy("BRANCH_ID", "BRANCH_NAME") \
    #     .agg(
    #         count("*").alias("TOTAL_TRANSACTIONS"),
    #         sum("AMOUNT").alias("TOTAL_AMOUNT")
    #     )
    # [DEPRECATED] Old logic above

    # [ADDED] New logic with operational details
    branch_summary = transaction_df.join(account_df, "ACCOUNT_ID") \
        .join(branch_df, "BRANCH_ID") \
        .groupBy("BRANCH_ID", "BRANCH_NAME") \
        .agg(
            count("*").alias("TOTAL_TRANSACTIONS"),
            sum("AMOUNT").alias("TOTAL_AMOUNT")
        )

    branch_summary = branch_summary.join(branch_operational_details_df, "BRANCH_ID", "left")

    branch_summary = branch_summary \
        .withColumn(
            "REGION",
            when(col("IS_ACTIVE") == "Y", col("REGION")).otherwise(None)  # [ADDED] Conditional REGION
        ) \
        .withColumn(
            "LAST_AUDIT_DATE",
            when(col("IS_ACTIVE") == "Y", col("LAST_AUDIT_DATE")).otherwise(None)  # [ADDED] Conditional LAST_AUDIT_DATE
        )

    # Select columns as per target DDL
    branch_summary = branch_summary.select(
        "BRANCH_ID", "BRANCH_NAME", "TOTAL_TRANSACTIONS", "TOTAL_AMOUNT", "REGION", "LAST_AUDIT_DATE"
    )
    return branch_summary

def write_to_delta_table(df: DataFrame, table_name: str):
    """
    Writes a DataFrame to a Delta table.
    """
    logger.info(f"Writing DataFrame to Delta table: {table_name}")
    try:
        df.write.format("delta") \
          .mode("overwrite") \
          .option("overwriteSchema", "true") \
          .saveAsTable(table_name)
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
        # [ADDED] Use sample data for self-contained ETL
        customer_df, account_df, transaction_df, branch_df, branch_operational_details_df = create_sample_data(spark)

        # [UNCHANGED] Create and write AML_CUSTOMER_TRANSACTIONS (not impacted by the delta)
        aml_transactions_df = customer_df.join(account_df, "CUSTOMER_ID") \
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
        write_to_delta_table(aml_transactions_df, "AML_CUSTOMER_TRANSACTIONS")

        # [MODIFIED] Create and write BRANCH_SUMMARY_REPORT with operational details
        branch_summary_df = create_branch_summary_report(
            transaction_df, account_df, branch_df, branch_operational_details_df)
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
