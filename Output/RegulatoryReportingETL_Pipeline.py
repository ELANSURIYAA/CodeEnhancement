#====================================================================
# Author: Ascendion AAVA
# Date: 
# Description: Enhanced PySpark ETL integrating BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT with conditional logic and compliance with new business requirements.
#====================================================================

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

# [ADDED] Sample data creation functions for self-contained ETL

def create_sample_customer_df(spark):
    data = [
        (1, "Alice", "alice@bank.com", "1234567890", "123 Main St", "2023-01-01"),
        (2, "Bob", "bob@bank.com", "2345678901", "456 Oak St", "2023-01-02")
    ]
    columns = ["CUSTOMER_ID", "NAME", "EMAIL", "PHONE", "ADDRESS", "CREATED_DATE"]
    return spark.createDataFrame(data, columns)

def create_sample_branch_df(spark):
    data = [
        (101, "Central", "CEN001", "CityA", "StateA", "CountryA"),
        (102, "West", "WST001", "CityB", "StateB", "CountryB")
    ]
    columns = ["BRANCH_ID", "BRANCH_NAME", "BRANCH_CODE", "CITY", "STATE", "COUNTRY"]
    return spark.createDataFrame(data, columns)

def create_sample_account_df(spark):
    data = [
        (1001, 1, 101, "ACC001", "SAVINGS", 5000.00, "2023-01-01"),
        (1002, 2, 102, "ACC002", "CURRENT", 15000.00, "2023-01-02")
    ]
    columns = ["ACCOUNT_ID", "CUSTOMER_ID", "BRANCH_ID", "ACCOUNT_NUMBER", "ACCOUNT_TYPE", "BALANCE", "OPENED_DATE"]
    return spark.createDataFrame(data, columns)

def create_sample_transaction_df(spark):
    data = [
        (50001, 1001, "DEPOSIT", 1000.00, "2023-02-01", "Salary"),
        (50002, 1002, "WITHDRAWAL", 500.00, "2023-02-02", "Bill Payment")
    ]
    columns = ["TRANSACTION_ID", "ACCOUNT_ID", "TRANSACTION_TYPE", "AMOUNT", "TRANSACTION_DATE", "DESCRIPTION"]
    return spark.createDataFrame(data, columns)

def create_sample_branch_operational_details_df(spark):
    data = [
        (101, "North", "John Doe", "2023-03-01", "Y"),
        (102, "West", "Jane Smith", "2023-03-05", "N")
    ]
    columns = ["BRANCH_ID", "REGION", "MANAGER_NAME", "LAST_AUDIT_DATE", "IS_ACTIVE"]
    return spark.createDataFrame(data, columns)

# [MODIFIED] Enhanced ETL logic for BRANCH_SUMMARY_REPORT

def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, branch_df: DataFrame, branch_op_df: DataFrame) -> DataFrame:
    """
    Creates the BRANCH_SUMMARY_REPORT DataFrame by aggregating transaction data at the branch level and integrating operational details.
    """
    logger.info("Creating Branch Summary Report DataFrame with operational details.")
    # [DEPRECATED] Old logic without operational details
    # old_branch_summary = transaction_df.join(account_df, "ACCOUNT_ID") \
    #     .join(branch_df, "BRANCH_ID") \
    #     .groupBy("BRANCH_ID", "BRANCH_NAME") \
    #     .agg(
    #         count("*").alias("TOTAL_TRANSACTIONS"),
    #         sum("AMOUNT").alias("TOTAL_AMOUNT")
    #     )
    # [MODIFIED] New logic includes join with BRANCH_OPERATIONAL_DETAILS
    branch_summary = transaction_df.join(account_df, "ACCOUNT_ID") \
        .join(branch_df, "BRANCH_ID") \
        .groupBy("BRANCH_ID", "BRANCH_NAME") \
        .agg(
            count("*").alias("TOTAL_TRANSACTIONS"),
            sum("AMOUNT").alias("TOTAL_AMOUNT")
        )
    branch_summary = branch_summary.join(branch_op_df, "BRANCH_ID", "left")
    branch_summary = branch_summary.withColumn(
        "REGION", when(col("IS_ACTIVE") == "Y", col("REGION")).otherwise(None)
    ).withColumn(
        "LAST_AUDIT_DATE", when(col("IS_ACTIVE") == "Y", col("LAST_AUDIT_DATE")).otherwise(None)
    )
    return branch_summary.select(
        "BRANCH_ID", "BRANCH_NAME", "TOTAL_TRANSACTIONS", "TOTAL_AMOUNT", "REGION", "LAST_AUDIT_DATE"
    )

# [UNCHANGED] AML_CUSTOMER_TRANSACTIONS logic remains as in previous version

def create_aml_customer_transactions(customer_df: DataFrame, account_df: DataFrame, transaction_df: DataFrame) -> DataFrame:
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

def write_to_delta_table(df: DataFrame, table_name: str):
    """
    Writes a DataFrame to a Delta table.
    """
    logger.info(f"Writing DataFrame to Delta table: {table_name}")
    try:
        df.write.format("delta") \
          .mode("overwrite") \
          .saveAsTable(table_name)
        logger.info(f"Successfully written data to {table_name}")
    except Exception as e:
        logger.error(f"Failed to write to Delta table {table_name}: {e}")
        raise

def main():
    """
    Main ETL execution function with sample data.
    """
    spark = None
    try:
        spark = get_spark_session()
        # [ADDED] Use sample dataframes instead of JDBC reads
        customer_df = create_sample_customer_df(spark)
        account_df = create_sample_account_df(spark)
        transaction_df = create_sample_transaction_df(spark)
        branch_df = create_sample_branch_df(spark)
        branch_op_df = create_sample_branch_operational_details_df(spark)
        # Create and write AML_CUSTOMER_TRANSACTIONS
        aml_transactions_df = create_aml_customer_transactions(customer_df, account_df, transaction_df)
        write_to_delta_table(aml_transactions_df, "AML_CUSTOMER_TRANSACTIONS")
        # Create and write BRANCH_SUMMARY_REPORT with operational details
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

#====================================================================
# Summary of Updates:
# - Integrated BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT logic.
# - Added conditional population of REGION and LAST_AUDIT_DATE based on IS_ACTIVE.
# - Deprecated legacy branch summary aggregation logic (commented out for traceability).
# - All changes annotated inline (# [ADDED], # [MODIFIED], # [DEPRECATED]).
# - Sample data generation for self-contained Databricks execution.
#====================================================================
