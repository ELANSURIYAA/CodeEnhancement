====================================================================
Author: Ascendion AAVA
Date: 
Description: Enhanced PySpark ETL for BRANCH_SUMMARY_REPORT with integration of BRANCH_OPERATIONAL_DETAILS and conditional population of REGION and LAST_AUDIT_DATE.
====================================================================

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum, when
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# [MODIFIED] Use getActiveSession for Spark Connect compatibility

def get_spark_session(app_name: str = "RegulatoryReportingETL") -> SparkSession:
    """
    Initializes and returns a Spark session.
    """
    try:
        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = SparkSession.builder \
                .appName(app_name) \
                .getOrCreate()
        logger.info("Spark session created successfully.")
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark session: {e}")
        raise

# [ADDED] Sample data creation functions (self-contained, no external JDBC)
def create_sample_data(spark: SparkSession):
    """
    Creates sample DataFrames for CUSTOMER, ACCOUNT, TRANSACTION, BRANCH, BRANCH_OPERATIONAL_DETAILS.
    """
    customer_schema = StructType([
        StructField("CUSTOMER_ID", IntegerType()),
        StructField("NAME", StringType()),
        StructField("EMAIL", StringType()),
        StructField("PHONE", StringType()),
        StructField("ADDRESS", StringType()),
        StructField("CREATED_DATE", StringType()),
    ])
    customer_data = [
        (1, "John Doe", "john@example.com", "1234567890", "123 Main St", "2023-01-01"),
        (2, "Jane Smith", "jane@example.com", "0987654321", "456 Elm St", "2023-02-01"),
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
        (101, 1, 10, "ACC123", "SAVINGS", 5000.0, "2023-01-10"),
        (102, 2, 20, "ACC456", "CURRENT", 7500.0, "2023-02-10"),
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
        (1001, 101, "DEPOSIT", 1000.0, "2023-01-15", "Initial deposit"),
        (1002, 102, "WITHDRAWAL", 500.0, "2023-02-15", "ATM withdrawal"),
        (1003, 101, "DEPOSIT", 2000.0, "2023-03-01", "Salary"),
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
        (10, "Central Branch", "CB001", "Metropolis", "StateA", "CountryX"),
        (20, "East Branch", "EB002", "Gotham", "StateB", "CountryY"),
    ]
    branch_df = spark.createDataFrame(branch_data, schema=branch_schema)

    branch_op_schema = StructType([
        StructField("BRANCH_ID", IntegerType()),
        StructField("REGION", StringType()),
        StructField("MANAGER_NAME", StringType()),
        StructField("LAST_AUDIT_DATE", StringType()),
        StructField("IS_ACTIVE", StringType()),
    ])
    branch_op_data = [
        (10, "North", "Alice", "2023-04-01", "Y"),
        (20, "South", "Bob", "2023-03-15", "N"),
    ]
    branch_op_df = spark.createDataFrame(branch_op_data, schema=branch_op_schema)

    return customer_df, account_df, transaction_df, branch_df, branch_op_df

# [MODIFIED] Enhanced branch summary to join with BRANCH_OPERATIONAL_DETAILS and conditionally populate REGION and LAST_AUDIT_DATE
def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, branch_df: DataFrame, branch_op_df: DataFrame) -> DataFrame:
    """
    Creates the BRANCH_SUMMARY_REPORT DataFrame by aggregating transaction data at the branch level and integrating operational details.
    """
    logger.info("Creating Branch Summary Report DataFrame with operational details.")
    branch_summary_df = transaction_df.join(account_df, "ACCOUNT_ID") \
        .join(branch_df, "BRANCH_ID") \
        .groupBy("BRANCH_ID", "BRANCH_NAME") \
        .agg(
            count("*").alias("TOTAL_TRANSACTIONS"),
            sum("AMOUNT").alias("TOTAL_AMOUNT")
        )
    # [ADDED] Join with operational details and conditionally populate REGION and LAST_AUDIT_DATE
    branch_summary_df = branch_summary_df.join(branch_op_df, "BRANCH_ID", "left")
    branch_summary_df = branch_summary_df.withColumn(
        "REGION", when(col("IS_ACTIVE") == "Y", col("REGION")).otherwise(None)
    ).withColumn(
        "LAST_AUDIT_DATE", when(col("IS_ACTIVE") == "Y", col("LAST_AUDIT_DATE")).otherwise(None)
    )
    # [DEPRECATED] Original logic without operational details
    # branch_summary_df = transaction_df.join(account_df, "ACCOUNT_ID") \
    #     .join(branch_df, "BRANCH_ID") \
    #     .groupBy("BRANCH_ID", "BRANCH_NAME") \
    #     .agg(
    #         count("*").alias("TOTAL_TRANSACTIONS"),
    #         sum("AMOUNT").alias("TOTAL_AMOUNT")
    #     )
    return branch_summary_df.select(
        "BRANCH_ID", "BRANCH_NAME", "TOTAL_TRANSACTIONS", "TOTAL_AMOUNT", "REGION", "LAST_AUDIT_DATE"
    )

# [ADDED] Write to Delta table simulation (show output instead of writing to Delta)
def write_to_delta_table(df: DataFrame, table_name: str):
    """
    Simulates writing a DataFrame to a Delta table by showing the output.
    """
    logger.info(f"Simulated write to Delta table: {table_name}")
    df.show(truncate=False)

# [MODIFIED] Main ETL execution function using sample data and enhanced logic
def main():
    """
    Main ETL execution function.
    """
    spark = None
    try:
        spark = get_spark_session()
        customer_df, account_df, transaction_df, branch_df, branch_op_df = create_sample_data(spark)

        # Create and write AML_CUSTOMER_TRANSACTIONS (unchanged)
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

# [SUMMARY OF UPDATION]
# - Integrated BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT logic
# - Added conditional population for REGION and LAST_AUDIT_DATE based on IS_ACTIVE
# - Deprecated original branch summary logic (commented out)
# - All changes annotated with [ADDED], [MODIFIED], [DEPRECATED]
# - Sample data generation for self-contained execution
# - Delta write simulated by .show()
