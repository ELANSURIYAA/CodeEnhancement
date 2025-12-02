====================================================================
Author: Ascendion AAVA
Date: 
Description: Enhanced PySpark ETL for BRANCH_SUMMARY_REPORT with BRANCH_OPERATIONAL_DETAILS integration
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
        if not spark:
            spark = SparkSession.builder.appName(app_name).getOrCreate()
        logger.info("Spark session created successfully.")
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark session: {e}")
        raise

def create_sample_data(spark: SparkSession):
    """
    [ADDED] Creates sample DataFrames to simulate Delta tables for testing.
    """
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
    import datetime

    customer_schema = StructType([
        StructField("CUSTOMER_ID", IntegerType()),
        StructField("NAME", StringType()),
        StructField("EMAIL", StringType()),
        StructField("PHONE", StringType()),
        StructField("ADDRESS", StringType()),
        StructField("CREATED_DATE", DateType())
    ])
    customer_data = [
        (1, "Alice", "alice@example.com", "1234567890", "Street 1", datetime.date(2023,1,1)),
        (2, "Bob", "bob@example.com", "0987654321", "Street 2", datetime.date(2023,2,1))
    ]
    customer_df = spark.createDataFrame(customer_data, customer_schema)

    branch_schema = StructType([
        StructField("BRANCH_ID", IntegerType()),
        StructField("BRANCH_NAME", StringType()),
        StructField("BRANCH_CODE", StringType()),
        StructField("CITY", StringType()),
        StructField("STATE", StringType()),
        StructField("COUNTRY", StringType())
    ])
    branch_data = [
        (101, "Main Branch", "MB01", "CityA", "StateA", "CountryA"),
        (102, "Branch2", "B02", "CityB", "StateB", "CountryB")
    ]
    branch_df = spark.createDataFrame(branch_data, branch_schema)

    account_schema = StructType([
        StructField("ACCOUNT_ID", IntegerType()),
        StructField("CUSTOMER_ID", IntegerType()),
        StructField("BRANCH_ID", IntegerType()),
        StructField("ACCOUNT_NUMBER", StringType()),
        StructField("ACCOUNT_TYPE", StringType()),
        StructField("BALANCE", DoubleType()),
        StructField("OPENED_DATE", DateType())
    ])
    account_data = [
        (1001, 1, 101, "AC001", "Savings", 5000.0, datetime.date(2023,3,1)),
        (1002, 2, 102, "AC002", "Current", 2000.0, datetime.date(2023,4,1))
    ]
    account_df = spark.createDataFrame(account_data, account_schema)

    transaction_schema = StructType([
        StructField("TRANSACTION_ID", IntegerType()),
        StructField("ACCOUNT_ID", IntegerType()),
        StructField("TRANSACTION_TYPE", StringType()),
        StructField("AMOUNT", DoubleType()),
        StructField("TRANSACTION_DATE", DateType()),
        StructField("DESCRIPTION", StringType())
    ])
    transaction_data = [
        (50001, 1001, "Deposit", 1000.0, datetime.date(2023,5,1), "Initial deposit"),
        (50002, 1002, "Withdrawal", 500.0, datetime.date(2023,5,2), "ATM withdrawal")
    ]
    transaction_df = spark.createDataFrame(transaction_data, transaction_schema)

    branch_op_schema = StructType([
        StructField("BRANCH_ID", IntegerType()),
        StructField("REGION", StringType()),
        StructField("MANAGER_NAME", StringType()),
        StructField("LAST_AUDIT_DATE", StringType()),
        StructField("IS_ACTIVE", StringType())
    ])
    branch_op_data = [
        (101, "North", "John Doe", "2023-05-10", "Y"),
        (102, "South", "Jane Smith", "2023-05-11", "N")
    ]
    branch_op_df = spark.createDataFrame(branch_op_data, branch_op_schema)
    return customer_df, account_df, transaction_df, branch_df, branch_op_df

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

def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, branch_df: DataFrame, branch_op_df: DataFrame) -> DataFrame:
    """
    Creates the BRANCH_SUMMARY_REPORT DataFrame by aggregating transaction data at the branch level and integrating BRANCH_OPERATIONAL_DETAILS.
    [MODIFIED] Now joins branch_op_df and conditionally populates REGION and LAST_AUDIT_DATE.
    """
    logger.info("Creating Branch Summary Report DataFrame with operational details.")
    base_df = transaction_df.join(account_df, "ACCOUNT_ID") \
                            .join(branch_df, "BRANCH_ID") \
                            .groupBy("BRANCH_ID", "BRANCH_NAME") \
                            .agg(
                                count("*").alias("TOTAL_TRANSACTIONS"),
                                sum("AMOUNT").alias("TOTAL_AMOUNT")
                            )
    # [MODIFIED] Legacy logic below commented out for audit traceability
    # base_df = transaction_df.join(account_df, "ACCOUNT_ID") \
    #                       .join(branch_df, "BRANCH_ID") \
    #                       .groupBy("BRANCH_ID", "BRANCH_NAME") \
    #                       .agg(
    #                           count("*").alias("TOTAL_TRANSACTIONS"),
    #                           sum("AMOUNT").alias("TOTAL_AMOUNT")
    #                       )
    # [ADDED] Join with BRANCH_OPERATIONAL_DETAILS and populate REGION & LAST_AUDIT_DATE when IS_ACTIVE = 'Y'
    final_df = base_df.join(branch_op_df, "BRANCH_ID", "left") \
        .withColumn("REGION", when(col("IS_ACTIVE") == "Y", col("REGION")).otherwise(None)) \
        .withColumn("LAST_AUDIT_DATE", when(col("IS_ACTIVE") == "Y", col("LAST_AUDIT_DATE")).otherwise(None))
    return final_df.select("BRANCH_ID", "BRANCH_NAME", "TOTAL_TRANSACTIONS", "TOTAL_AMOUNT", "REGION", "LAST_AUDIT_DATE")

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
    Main ETL execution function.
    """
    spark = None
    try:
        spark = get_spark_session()
        # [ADDED] Use sample data for self-contained execution
        customer_df, account_df, transaction_df, branch_df, branch_op_df = create_sample_data(spark)
        # Create and write AML_CUSTOMER_TRANSACTIONS
        aml_transactions_df = create_aml_customer_transactions(customer_df, account_df, transaction_df)
        write_to_delta_table(aml_transactions_df, "AML_CUSTOMER_TRANSACTIONS")
        # Create and write BRANCH_SUMMARY_REPORT
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
