====================================================================
Author: Ascendion AAVA
Date: <Leave it blank>
Description: Complete Enhanced PySpark ETL pipeline integrating BRANCH_OPERATIONAL_DETAILS
====================================================================

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum as spark_sum, when, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from datetime import datetime, date

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_spark_session(app_name: str = "RegulatoryReportingETL") -> SparkSession:
    """Initializes Spark session compatible with Spark Connect. [MODIFIED]"""
    try:
        try:
            spark = SparkSession.getActiveSession()
            if spark is not None:
                logger.info("Using existing active Spark session.")
                return spark
        except:
            pass
        
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
        
        logger.info("Spark session created successfully.")
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark session: {e}")
        raise

def create_sample_data(spark: SparkSession) -> tuple:
    """Creates sample data for testing. [ADDED]"""
    logger.info("Creating sample data for testing")
    
    # Sample data definitions
    customer_data = [(1, "John Doe", "john@email.com", "123-456-7890", "123 Main St", date(2023, 1, 15)),
                     (2, "Jane Smith", "jane@email.com", "098-765-4321", "456 Oak Ave", date(2023, 2, 20)),
                     (3, "Bob Johnson", "bob@email.com", "555-123-4567", "789 Pine Rd", date(2023, 3, 10))]
    
    branch_data = [(101, "Downtown Branch", "DT001", "New York", "NY", "USA"),
                   (102, "Uptown Branch", "UT002", "Los Angeles", "CA", "USA"),
                   (103, "Central Branch", "CT003", "Chicago", "IL", "USA")]
    
    account_data = [(1001, 1, 101, "ACC001001", "CHECKING", 5000.00, date(2023, 1, 16)),
                    (1002, 2, 102, "ACC002002", "SAVINGS", 15000.00, date(2023, 2, 21)),
                    (1003, 3, 103, "ACC003003", "CHECKING", 7500.00, date(2023, 3, 11)),
                    (1004, 1, 101, "ACC001004", "SAVINGS", 25000.00, date(2023, 1, 20))]
    
    transaction_data = [(10001, 1001, "DEPOSIT", 1000.00, date(2023, 4, 1), "Salary deposit"),
                        (10002, 1001, "WITHDRAWAL", 500.00, date(2023, 4, 5), "ATM withdrawal"),
                        (10003, 1002, "DEPOSIT", 2000.00, date(2023, 4, 2), "Check deposit"),
                        (10004, 1003, "TRANSFER", 750.00, date(2023, 4, 3), "Online transfer"),
                        (10005, 1004, "DEPOSIT", 5000.00, date(2023, 4, 4), "Investment return")]
    
    # [ADDED] - New source table data
    branch_operational_data = [(101, "Northeast", "Alice Manager", date(2023, 3, 15), "Y"),
                               (102, "West Coast", "Bob Manager", date(2023, 2, 28), "Y"),
                               (103, "Midwest", "Charlie Manager", date(2023, 1, 20), "N")]
    
    # Create schemas
    customer_schema = StructType([StructField("CUSTOMER_ID", IntegerType(), True),
                                  StructField("NAME", StringType(), True),
                                  StructField("EMAIL", StringType(), True),
                                  StructField("PHONE", StringType(), True),
                                  StructField("ADDRESS", StringType(), True),
                                  StructField("CREATED_DATE", DateType(), True)])
    
    branch_schema = StructType([StructField("BRANCH_ID", IntegerType(), True),
                                StructField("BRANCH_NAME", StringType(), True),
                                StructField("BRANCH_CODE", StringType(), True),
                                StructField("CITY", StringType(), True),
                                StructField("STATE", StringType(), True),
                                StructField("COUNTRY", StringType(), True)])
    
    account_schema = StructType([StructField("ACCOUNT_ID", IntegerType(), True),
                                 StructField("CUSTOMER_ID", IntegerType(), True),
                                 StructField("BRANCH_ID", IntegerType(), True),
                                 StructField("ACCOUNT_NUMBER", StringType(), True),
                                 StructField("ACCOUNT_TYPE", StringType(), True),
                                 StructField("BALANCE", DoubleType(), True),
                                 StructField("OPENED_DATE", DateType(), True)])
    
    transaction_schema = StructType([StructField("TRANSACTION_ID", IntegerType(), True),
                                     StructField("ACCOUNT_ID", IntegerType(), True),
                                     StructField("TRANSACTION_TYPE", StringType(), True),
                                     StructField("AMOUNT", DoubleType(), True),
                                     StructField("TRANSACTION_DATE", DateType(), True),
                                     StructField("DESCRIPTION", StringType(), True)])
    
    branch_operational_schema = StructType([StructField("BRANCH_ID", IntegerType(), True),
                                            StructField("REGION", StringType(), True),
                                            StructField("MANAGER_NAME", StringType(), True),
                                            StructField("LAST_AUDIT_DATE", DateType(), True),
                                            StructField("IS_ACTIVE", StringType(), True)])
    
    # Create DataFrames
    customer_df = spark.createDataFrame(customer_data, customer_schema)
    branch_df = spark.createDataFrame(branch_data, branch_schema)
    account_df = spark.createDataFrame(account_data, account_schema)
    transaction_df = spark.createDataFrame(transaction_data, transaction_schema)
    branch_operational_df = spark.createDataFrame(branch_operational_data, branch_operational_schema)
    
    return customer_df, branch_df, account_df, transaction_df, branch_operational_df

def create_aml_customer_transactions(customer_df: DataFrame, account_df: DataFrame, transaction_df: DataFrame) -> DataFrame:
    """Creates AML_CUSTOMER_TRANSACTIONS DataFrame. [UNCHANGED]"""
    logger.info("Creating AML Customer Transactions DataFrame.")
    return customer_df.join(account_df, "CUSTOMER_ID") \
                      .join(transaction_df, "ACCOUNT_ID") \
                      .select(col("CUSTOMER_ID"), col("NAME"), col("ACCOUNT_ID"),
                              col("TRANSACTION_ID"), col("AMOUNT"), col("TRANSACTION_TYPE"),
                              col("TRANSACTION_DATE"))

def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, 
                                 branch_df: DataFrame, branch_operational_df: DataFrame) -> DataFrame:
    """Creates enhanced BRANCH_SUMMARY_REPORT with operational details. [MODIFIED]"""
    logger.info("Creating Enhanced Branch Summary Report DataFrame.")
    
    # [UNCHANGED] - Original aggregation logic preserved
    base_summary = transaction_df.join(account_df, "ACCOUNT_ID") \
                                 .join(branch_df, "BRANCH_ID") \
                                 .groupBy("BRANCH_ID", "BRANCH_NAME") \
                                 .agg(count("*").alias("TOTAL_TRANSACTIONS"),
                                      spark_sum("AMOUNT").alias("TOTAL_AMOUNT"))
    
    # [ADDED] - Integration with BRANCH_OPERATIONAL_DETAILS
    enhanced_summary = base_summary.join(
        branch_operational_df, 
        base_summary["BRANCH_ID"] == branch_operational_df["BRANCH_ID"], 
        "left"
    ).select(
        base_summary["BRANCH_ID"],
        base_summary["BRANCH_NAME"],
        base_summary["TOTAL_TRANSACTIONS"],
        base_summary["TOTAL_AMOUNT"],
        # [ADDED] - Conditional population based on IS_ACTIVE = 'Y'
        when(branch_operational_df["IS_ACTIVE"] == "Y", 
             branch_operational_df["REGION"]).otherwise(lit(None)).alias("REGION"),
        when(branch_operational_df["IS_ACTIVE"] == "Y", 
             branch_operational_df["LAST_AUDIT_DATE"]).otherwise(lit(None)).alias("LAST_AUDIT_DATE")
    )
    
    logger.info("Enhanced branch summary report created successfully.")
    return enhanced_summary

def write_to_delta_table(df: DataFrame, table_path: str, mode: str = "overwrite"):
    """Writes DataFrame to Delta table. [MODIFIED]"""
    logger.info(f"Writing DataFrame to Delta table at: {table_path}")
    try:
        df.write.format("delta").mode(mode).option("mergeSchema", "true").save(table_path)
        logger.info(f"Successfully written data to Delta table at {table_path}")
    except Exception as e:
        logger.error(f"Failed to write to Delta table at {table_path}: {e}")
        raise

def validate_data_quality(df: DataFrame, table_name: str) -> bool:
    """Performs data quality validations. [ADDED]"""
    logger.info(f"Performing data quality validation for {table_name}")
    try:
        row_count = df.count()
        if row_count == 0:
            logger.warning(f"{table_name} is empty")
            return False
        
        if table_name == "BRANCH_SUMMARY_REPORT":
            null_branch_ids = df.filter(col("BRANCH_ID").isNull()).count()
            if null_branch_ids > 0:
                logger.error(f"{table_name} contains {null_branch_ids} null BRANCH_ID values")
                return False
        
        logger.info(f"Data quality validation passed for {table_name}. Row count: {row_count}")
        return True
    except Exception as e:
        logger.error(f"Data quality validation failed for {table_name}: {e}")
        return False

def main():
    """Main ETL execution function. [MODIFIED]"""
    spark = None
    try:
        spark = get_spark_session()
        
        # [MODIFIED] - Using sample data for self-contained execution
        customer_df, branch_df, account_df, transaction_df, branch_operational_df = create_sample_data(spark)
        
        # Create and validate AML_CUSTOMER_TRANSACTIONS
        aml_transactions_df = create_aml_customer_transactions(customer_df, account_df, transaction_df)
        if validate_data_quality(aml_transactions_df, "AML_CUSTOMER_TRANSACTIONS"):
            write_to_delta_table(aml_transactions_df, "/tmp/delta/aml_customer_transactions")
        
        # [MODIFIED] - Create enhanced BRANCH_SUMMARY_REPORT with operational details
        branch_summary_df = create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_df)
        if validate_data_quality(branch_summary_df, "BRANCH_SUMMARY_REPORT"):
            write_to_delta_table(branch_summary_df, "/tmp/delta/branch_summary_report")
        
        logger.info("ETL job completed successfully with enhanced functionality.")
        
        # Display results for verification
        print("\n=== ENHANCED BRANCH SUMMARY REPORT ===")
        branch_summary_df.show(truncate=False)
        
    except Exception as e:
        logger.error(f"ETL job failed with exception: {e}")
        raise
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped.")

if __name__ == "__main__":
    main()