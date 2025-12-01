====================================================================
Author: Ascendion AAVA
Date: <Leave it blank>
Description: Enhanced PySpark ETL pipeline integrating BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT
====================================================================

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum, when, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, DateType, LongType, DoubleType
from datetime import date, datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_spark_session(app_name: str = "RegulatoryReportingETL") -> SparkSession:
    """
    Initializes and returns a Spark session compatible with Spark Connect.
    """
    try:
        # [MODIFIED] - Updated to use getActiveSession() for Spark Connect compatibility
        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = SparkSession.builder \
                .appName(app_name) \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .getOrCreate()
        
        # [MODIFIED] - Removed sparkContext calls for Spark Connect compatibility
        # spark.sparkContext.setLogLevel("WARN")
        spark.conf.set("spark.sql.adaptive.enabled", "true")
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        
        logger.info("Spark session created successfully.")
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark session: {e}")
        raise

def create_sample_data(spark: SparkSession) -> tuple:
    """
    Creates sample DataFrames for testing the ETL pipeline.
    [ADDED] - New function to create sample data for self-contained execution
    """
    logger.info("Creating sample data for testing")
    
    # Customer sample data
    customer_schema = StructType([
        StructField("CUSTOMER_ID", IntegerType(), True),
        StructField("NAME", StringType(), True),
        StructField("EMAIL", StringType(), True),
        StructField("PHONE", StringType(), True),
        StructField("ADDRESS", StringType(), True),
        StructField("CREATED_DATE", DateType(), True)
    ])
    
    customer_data = [
        (1, "John Doe", "john@email.com", "123-456-7890", "123 Main St", date(2023, 1, 15)),
        (2, "Jane Smith", "jane@email.com", "098-765-4321", "456 Oak Ave", date(2023, 2, 20)),
        (3, "Bob Johnson", "bob@email.com", "555-123-4567", "789 Pine Rd", date(2023, 3, 10))
    ]
    
    customer_df = spark.createDataFrame(customer_data, customer_schema)
    
    # Branch sample data
    branch_schema = StructType([
        StructField("BRANCH_ID", IntegerType(), True),
        StructField("BRANCH_NAME", StringType(), True),
        StructField("BRANCH_CODE", StringType(), True),
        StructField("CITY", StringType(), True),
        StructField("STATE", StringType(), True),
        StructField("COUNTRY", StringType(), True)
    ])
    
    branch_data = [
        (101, "Downtown Branch", "DT001", "New York", "NY", "USA"),
        (102, "Uptown Branch", "UT002", "Los Angeles", "CA", "USA"),
        (103, "Central Branch", "CT003", "Chicago", "IL", "USA")
    ]
    
    branch_df = spark.createDataFrame(branch_data, branch_schema)
    
    # Account sample data
    account_schema = StructType([
        StructField("ACCOUNT_ID", IntegerType(), True),
        StructField("CUSTOMER_ID", IntegerType(), True),
        StructField("BRANCH_ID", IntegerType(), True),
        StructField("ACCOUNT_NUMBER", StringType(), True),
        StructField("ACCOUNT_TYPE", StringType(), True),
        StructField("BALANCE", DecimalType(15, 2), True),
        StructField("OPENED_DATE", DateType(), True)
    ])
    
    account_data = [
        (1001, 1, 101, "ACC001001", "SAVINGS", 5000.00, date(2023, 1, 16)),
        (1002, 2, 102, "ACC001002", "CHECKING", 3000.00, date(2023, 2, 21)),
        (1003, 3, 103, "ACC001003", "SAVINGS", 7500.00, date(2023, 3, 11)),
        (1004, 1, 101, "ACC001004", "CHECKING", 2500.00, date(2023, 1, 20))
    ]
    
    account_df = spark.createDataFrame(account_data, account_schema)
    
    # Transaction sample data
    transaction_schema = StructType([
        StructField("TRANSACTION_ID", IntegerType(), True),
        StructField("ACCOUNT_ID", IntegerType(), True),
        StructField("TRANSACTION_TYPE", StringType(), True),
        StructField("AMOUNT", DecimalType(15, 2), True),
        StructField("TRANSACTION_DATE", DateType(), True),
        StructField("DESCRIPTION", StringType(), True)
    ])
    
    transaction_data = [
        (10001, 1001, "DEPOSIT", 1000.00, date(2023, 4, 1), "Salary deposit"),
        (10002, 1001, "WITHDRAWAL", 200.00, date(2023, 4, 2), "ATM withdrawal"),
        (10003, 1002, "DEPOSIT", 500.00, date(2023, 4, 1), "Check deposit"),
        (10004, 1003, "TRANSFER", 1500.00, date(2023, 4, 3), "Wire transfer"),
        (10005, 1004, "DEPOSIT", 800.00, date(2023, 4, 2), "Cash deposit")
    ]
    
    transaction_df = spark.createDataFrame(transaction_data, transaction_schema)
    
    # [ADDED] - Branch Operational Details sample data (new source table)
    branch_operational_schema = StructType([
        StructField("BRANCH_ID", IntegerType(), True),
        StructField("REGION", StringType(), True),
        StructField("MANAGER_NAME", StringType(), True),
        StructField("LAST_AUDIT_DATE", DateType(), True),
        StructField("IS_ACTIVE", StringType(), True)
    ])
    
    branch_operational_data = [
        (101, "Northeast", "Alice Manager", date(2023, 3, 15), "Y"),
        (102, "West", "Bob Manager", date(2023, 2, 28), "Y"),
        (103, "Midwest", "Carol Manager", date(2023, 1, 20), "N")  # Inactive branch for testing
    ]
    
    branch_operational_df = spark.createDataFrame(branch_operational_data, branch_operational_schema)
    
    return customer_df, account_df, transaction_df, branch_df, branch_operational_df

# [DEPRECATED] - Original JDBC-based read_table function
# def read_table(spark: SparkSession, jdbc_url: str, table_name: str, connection_properties: dict) -> DataFrame:
#     """
#     Reads a table from a JDBC source into a DataFrame.
#     This function is deprecated in favor of Delta table reads and sample data creation.
#     """
#     logger.info(f"Reading table: {table_name}")
#     try:
#         df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
#         return df
#     except Exception as e:
#         logger.error(f"Failed to read table {table_name}: {e}")
#         raise

def read_delta_table(spark: SparkSession, table_name: str) -> DataFrame:
    """
    [ADDED] - Reads a Delta table into a DataFrame.
    
    :param spark: The SparkSession object.
    :param table_name: The name of the Delta table to read.
    :return: A Spark DataFrame containing the table data.
    """
    logger.info(f"Reading Delta table: {table_name}")
    try:
        df = spark.read.format("delta").table(table_name)
        logger.info(f"Successfully read {df.count()} rows from {table_name}")
        return df
    except Exception as e:
        logger.error(f"Failed to read Delta table {table_name}: {e}")
        raise

def create_aml_customer_transactions(customer_df: DataFrame, account_df: DataFrame, transaction_df: DataFrame) -> DataFrame:
    """
    Creates the AML_CUSTOMER_TRANSACTIONS DataFrame by joining customer, account, and transaction data.
    
    :param customer_df: DataFrame with customer data.
    :param account_df: DataFrame with account data.
    :param transaction_df: DataFrame with transaction data.
    :return: A DataFrame ready for the AML customer transactions report.
    """
    logger.info("Creating AML Customer Transactions DataFrame.")
    
    # [MODIFIED] - Added data validation and improved join logic
    # Validate input DataFrames
    if customer_df.count() == 0 or account_df.count() == 0 or transaction_df.count() == 0:
        logger.warning("One or more input DataFrames are empty")
    
    result_df = customer_df.join(account_df, "CUSTOMER_ID", "inner") \
                          .join(transaction_df, "ACCOUNT_ID", "inner") \
                          .select(
                              col("CUSTOMER_ID"),
                              col("NAME"),
                              col("ACCOUNT_ID"),
                              col("TRANSACTION_ID"),
                              col("AMOUNT"),
                              col("TRANSACTION_TYPE"),
                              col("TRANSACTION_DATE")
                          )
    
    logger.info(f"AML Customer Transactions created with {result_df.count()} records")
    return result_df

def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, branch_df: DataFrame, branch_operational_df: DataFrame) -> DataFrame:
    """
    [MODIFIED] - Enhanced to integrate BRANCH_OPERATIONAL_DETAILS
    Creates the BRANCH_SUMMARY_REPORT DataFrame by aggregating transaction data at the branch level
    and integrating operational details.
    
    :param transaction_df: DataFrame with transaction data.
    :param account_df: DataFrame with account data.
    :param branch_df: DataFrame with branch data.
    :param branch_operational_df: DataFrame with branch operational details.
    :return: A DataFrame containing the enhanced branch summary report.
    """
    logger.info("Creating Enhanced Branch Summary Report DataFrame with operational details.")
    
    # [MODIFIED] - Enhanced aggregation logic with operational details integration
    # Step 1: Create base branch summary (existing logic)
    base_summary = transaction_df.join(account_df, "ACCOUNT_ID", "inner") \
                                 .join(branch_df, "BRANCH_ID", "inner") \
                                 .groupBy("BRANCH_ID", "BRANCH_NAME") \
                                 .agg(
                                     count("*").alias("TOTAL_TRANSACTIONS"),
                                     sum("AMOUNT").alias("TOTAL_AMOUNT")
                                 )
    
    # [ADDED] - Step 2: Join with branch operational details and apply conditional logic
    enhanced_summary = base_summary.join(
        branch_operational_df, 
        "BRANCH_ID", 
        "left"  # Left join to preserve branches without operational details
    ).select(
        col("BRANCH_ID").cast(LongType()),  # Cast to match target schema
        col("BRANCH_NAME"),
        col("TOTAL_TRANSACTIONS"),
        col("TOTAL_AMOUNT").cast(DoubleType()),  # Cast to match target schema
        # [ADDED] - Conditional population based on IS_ACTIVE = 'Y'
        when(col("IS_ACTIVE") == "Y", col("REGION")).otherwise(lit(None)).alias("REGION"),
        when(col("IS_ACTIVE") == "Y", col("LAST_AUDIT_DATE").cast(StringType())).otherwise(lit(None)).alias("LAST_AUDIT_DATE")
    )
    
    # [ADDED] - Data validation
    total_records = enhanced_summary.count()
    active_branches = enhanced_summary.filter(col("REGION").isNotNull()).count()
    
    logger.info(f"Branch Summary Report created with {total_records} total branches, {active_branches} active branches")
    
    return enhanced_summary

def write_to_delta_table(df: DataFrame, table_name: str, mode: str = "overwrite"):
    """
    [MODIFIED] - Enhanced Delta table writing with better error handling and logging
    Writes a DataFrame to a Delta table.
    
    :param df: The DataFrame to write.
    :param table_name: The name of the target Delta table.
    :param mode: Write mode (overwrite, append, etc.)
    """
    logger.info(f"Writing DataFrame to Delta table: {table_name} with mode: {mode}")
    try:
        # [ADDED] - Enhanced Delta write with optimizations
        df.write.format("delta") \
          .mode(mode) \
          .option("mergeSchema", "true") \
          .option("overwriteSchema", "true" if mode == "overwrite" else "false") \
          .saveAsTable(table_name)
        
        # [ADDED] - Post-write validation
        written_count = spark.read.format("delta").table(table_name).count()
        logger.info(f"Successfully written {written_count} records to {table_name}")
        
    except Exception as e:
        logger.error(f"Failed to write to Delta table {table_name}: {e}")
        raise

def validate_data_quality(df: DataFrame, table_name: str) -> bool:
    """
    [ADDED] - Data quality validation function
    Performs basic data quality checks on the DataFrame.
    
    :param df: DataFrame to validate
    :param table_name: Name of the table for logging
    :return: Boolean indicating if validation passed
    """
    logger.info(f"Performing data quality validation for {table_name}")
    
    try:
        # Check for null values in key columns
        if table_name == "BRANCH_SUMMARY_REPORT":
            null_branch_ids = df.filter(col("BRANCH_ID").isNull()).count()
            null_branch_names = df.filter(col("BRANCH_NAME").isNull()).count()
            
            if null_branch_ids > 0 or null_branch_names > 0:
                logger.warning(f"Found null values in key columns: BRANCH_ID({null_branch_ids}), BRANCH_NAME({null_branch_names})")
                return False
        
        # Check for negative amounts
        if "TOTAL_AMOUNT" in df.columns:
            negative_amounts = df.filter(col("TOTAL_AMOUNT") < 0).count()
            if negative_amounts > 0:
                logger.warning(f"Found {negative_amounts} records with negative total amounts")
        
        logger.info(f"Data quality validation passed for {table_name}")
        return True
        
    except Exception as e:
        logger.error(f"Data quality validation failed for {table_name}: {e}")
        return False

def main():
    """
    [MODIFIED] - Enhanced main ETL execution function with new source integration
    """
    spark = None
    try:
        spark = get_spark_session()
        
        # [MODIFIED] - Using sample data instead of JDBC connections for self-contained execution
        logger.info("Starting ETL job with sample data")
        
        # Create sample data
        customer_df, account_df, transaction_df, branch_df, branch_operational_df = create_sample_data(spark)
        
        # [DEPRECATED] - Original JDBC connection logic
        # # JDBC connection properties
        # # TODO: Replace with secure credential management (e.g., Databricks Secrets, Azure Key Vault)
        # jdbc_url = "jdbc:oracle:thin:@your_oracle_host:1521:orcl"
        # connection_properties = {
        #     "user": "your_user",
        #     "password": "your_password",
        #     "driver": "oracle.jdbc.driver.OracleDriver" # Ensure the driver is available in the classpath
        # }
        
        # # Read source tables
        # customer_df = read_table(spark, jdbc_url, "CUSTOMER", connection_properties)
        # account_df = read_table(spark, jdbc_url, "ACCOUNT", connection_properties)
        # transaction_df = read_table(spark, jdbc_url, "TRANSACTION", connection_properties)
        # branch_df = read_table(spark, jdbc_url, "BRANCH", connection_properties)
        # # [ADDED] - Read new source table
        # branch_operational_df = read_table(spark, jdbc_url, "BRANCH_OPERATIONAL_DETAILS", connection_properties)
        
        # Create and write AML_CUSTOMER_TRANSACTIONS (existing functionality)
        aml_transactions_df = create_aml_customer_transactions(customer_df, account_df, transaction_df)
        
        # [ADDED] - Data quality validation
        if validate_data_quality(aml_transactions_df, "AML_CUSTOMER_TRANSACTIONS"):
            write_to_delta_table(aml_transactions_df, "AML_CUSTOMER_TRANSACTIONS")
        else:
            logger.error("Data quality validation failed for AML_CUSTOMER_TRANSACTIONS")
            raise Exception("Data quality validation failed")
        
        # [MODIFIED] - Enhanced branch summary report creation with new source
        branch_summary_df = create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_df)
        
        # [ADDED] - Data quality validation for enhanced report
        if validate_data_quality(branch_summary_df, "BRANCH_SUMMARY_REPORT"):
            write_to_delta_table(branch_summary_df, "workspace.default.branch_summary_report")
        else:
            logger.error("Data quality validation failed for BRANCH_SUMMARY_REPORT")
            raise Exception("Data quality validation failed")
        
        # [ADDED] - Display sample results for verification
        logger.info("Displaying sample results:")
        print("\n=== BRANCH_SUMMARY_REPORT Sample Results ===")
        branch_summary_df.show(10, truncate=False)
        
        print("\n=== Schema Information ===")
        branch_summary_df.printSchema()
        
        logger.info("ETL job completed successfully with enhanced functionality.")
        
    except Exception as e:
        logger.error(f"ETL job failed with exception: {e}")
        raise
    finally:
        if spark:
            # [MODIFIED] - Graceful session cleanup
            logger.info("ETL job execution completed.")
            # Note: In Databricks, we typically don't stop the session
            # spark.stop()

if __name__ == "__main__":
    main()
