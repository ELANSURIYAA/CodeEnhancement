====================================================================
Author: Ascendion AAVA
Date: 
Description: Enhanced PySpark ETL integrating BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT with conditional logic for REGION and LAST_AUDIT_DATE.
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
        # [MODIFIED] Use getActiveSession() for Spark Connect compatibility
        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = SparkSession.builder.appName(app_name).getOrCreate()
        logger.info("Spark session created successfully.")
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark session: {e}")
        raise

# [DEPRECATED] Legacy JDBC read function (commented out for auditing)
# def read_table(spark: SparkSession, jdbc_url: str, table_name: str, connection_properties: dict) -> DataFrame:
#     """
#     Reads a table from a JDBC source into a DataFrame.
#     """
#     logger.info(f"Reading table: {table_name}")
#     try:
#         df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
#         return df
#     except Exception as e:
#         logger.error(f"Failed to read table {table_name}: {e}")
#         raise

# [ADDED] Sample data creation functions for self-contained execution

def create_sample_customer_df(spark):
    data = [
        (1, "Alice", "alice@email.com", "1111111111", "Address A", "2024-05-01"),
        (2, "Bob", "bob@email.com", "2222222222", "Address B", "2024-05-02")
    ]
    columns = ["CUSTOMER_ID", "NAME", "EMAIL", "PHONE", "ADDRESS", "CREATED_DATE"]
    return spark.createDataFrame(data, columns)

def create_sample_branch_df(spark):
    data = [
        (101, "Main Branch", "MB001", "CityA", "StateA", "CountryA"),
        (102, "West Branch", "WB002", "CityB", "StateB", "CountryB")
    ]
    columns = ["BRANCH_ID", "BRANCH_NAME", "BRANCH_CODE", "CITY", "STATE", "COUNTRY"]
    return spark.createDataFrame(data, columns)

def create_sample_account_df(spark):
    data = [
        (1001, 1, 101, "ACC001", "Savings", 5000.0, "2024-05-01"),
        (1002, 2, 102, "ACC002", "Checking", 3000.0, "2024-05-02")
    ]
    columns = ["ACCOUNT_ID", "CUSTOMER_ID", "BRANCH_ID", "ACCOUNT_NUMBER", "ACCOUNT_TYPE", "BALANCE", "OPENED_DATE"]
    return spark.createDataFrame(data, columns)

def create_sample_transaction_df(spark):
    data = [
        (50001, 1001, "Deposit", 2000.0, "2024-05-10", "Salary"),
        (50002, 1002, "Withdrawal", 1500.0, "2024-05-11", "Bill Payment")
    ]
    columns = ["TRANSACTION_ID", "ACCOUNT_ID", "TRANSACTION_TYPE", "AMOUNT", "TRANSACTION_DATE", "DESCRIPTION"]
    return spark.createDataFrame(data, columns)

def create_sample_branch_operational_details_df(spark):
    data = [
        (101, "North", "John Doe", "2024-05-15", "Y"),
        (102, "West", "Jane Smith", "2024-05-16", "N")
    ]
    columns = ["BRANCH_ID", "REGION", "MANAGER_NAME", "LAST_AUDIT_DATE", "IS_ACTIVE"]
    return spark.createDataFrame(data, columns)

# [UNCHANGED] AML customer transactions logic

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

# [DEPRECATED] Old branch summary report logic (commented for traceability)
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

# [ADDED] Enhanced branch summary report integrating BRANCH_OPERATIONAL_DETAILS

def create_branch_summary_report(
    transaction_df: DataFrame,
    account_df: DataFrame,
    branch_df: DataFrame,
    branch_operational_details_df: DataFrame
) -> DataFrame:
    """
    Creates the BRANCH_SUMMARY_REPORT DataFrame by aggregating transaction data at the branch level and integrating operational details.
    """
    logger.info("Creating Branch Summary Report DataFrame with Operational Details.")
    base_df = transaction_df.join(account_df, "ACCOUNT_ID") \
                           .join(branch_df, "BRANCH_ID") \
                           .groupBy("BRANCH_ID", "BRANCH_NAME") \
                           .agg(
                               count("*").alias("TOTAL_TRANSACTIONS"),
                               sum("AMOUNT").alias("TOTAL_AMOUNT")
                           )
    # [ADDED] Join with branch_operational_details and conditional population
    merged_df = base_df.join(branch_operational_details_df, "BRANCH_ID", "left")
    merged_df = merged_df.withColumn(
        "REGION",
        when(col("IS_ACTIVE") == "Y", col("REGION")).otherwise(None)
    ).withColumn(
        "LAST_AUDIT_DATE",
        when(col("IS_ACTIVE") == "Y", col("LAST_AUDIT_DATE")).otherwise(None)
    )
    # Select only required columns for target schema
    return merged_df.select(
        "BRANCH_ID", "BRANCH_NAME", "TOTAL_TRANSACTIONS", "TOTAL_AMOUNT", "REGION", "LAST_AUDIT_DATE"
    )

# [UNCHANGED] Write to Delta table

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

# [MODIFIED] Main function uses sample data for self-contained execution

def main():
    """
    Main ETL execution function.
    """
    spark = None
    try:
        spark = get_spark_session()
        # [ADDED] Create sample dataframes
        customer_df = create_sample_customer_df(spark)
        account_df = create_sample_account_df(spark)
        transaction_df = create_sample_transaction_df(spark)
        branch_df = create_sample_branch_df(spark)
        branch_operational_details_df = create_sample_branch_operational_details_df(spark)

        # Create and write AML_CUSTOMER_TRANSACTIONS
        aml_transactions_df = create_aml_customer_transactions(customer_df, account_df, transaction_df)
        write_to_delta_table(aml_transactions_df, "AML_CUSTOMER_TRANSACTIONS")

        # [MODIFIED] Enhanced branch summary report logic
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
