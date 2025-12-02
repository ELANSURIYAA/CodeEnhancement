====================================================================
Author: Ascendion AAVA
Date: <Leave it blank>
Description: Enhanced PySpark ETL pipeline integrating BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT
====================================================================

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum as spark_sum, when, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, LongType
from datetime import datetime, date

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_spark_session(app_name: str = "RegulatoryReportingETL") -> SparkSession:
    """
    Initializes and returns a Spark session compatible with Databricks and Delta.
    """
    try:
        # [MODIFIED] - Updated to use getActiveSession() for Spark Connect compatibility
        # Original code: spark = SparkSession.builder.appName(app_name).enableHiveSupport().getOrCreate()
        try:
            spark = SparkSession.getActiveSession()
            if spark is None:
                spark = SparkSession.builder.appName(app_name).getOrCreate()
        except:
            spark = SparkSession.builder.appName(app_name).getOrCreate()
        
        # [MODIFIED] - Removed sparkContext calls for Spark Connect compatibility
        # Original code: spark.sparkContext.setLogLevel("WARN")
        spark.conf.set("spark.sql.adaptive.enabled", "true")
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        logger.info("Spark session created successfully.")
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark session: {e}")
        raise

def create_sample_data(spark: SparkSession) -> tuple:
    """
    [ADDED] - Creates sample data for testing purposes.
    This function generates sample DataFrames to simulate the source tables.
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
        (101, "Downtown Branch", "DTN001", "New York", "NY", "USA"),
        (102, "Uptown Branch", "UPT002", "Los Angeles", "CA", "USA"),
        (103, "Central Branch", "CTR003", "Chicago", "IL", "USA")
    ]
    
    # Account sample data
    account_schema = StructType([
        StructField("ACCOUNT_ID", IntegerType(), True),
        StructField("CUSTOMER_ID", IntegerType(), True),
        StructField("BRANCH_ID", IntegerType(), True),
        StructField("ACCOUNT_NUMBER", StringType(), True),
        StructField("ACCOUNT_TYPE", StringType(), True),
        StructField("BALANCE", DoubleType(), True),
        StructField("OPENED_DATE", DateType(), True)
    ])
    
    account_data = [
        (1001, 1, 101, "ACC001001", "SAVINGS", 5000.00, date(2023, 1, 20)),
        (1002, 2, 102, "ACC002002", "CHECKING", 3000.00, date(2023, 2, 25)),
        (1003, 3, 103, "ACC003003", "SAVINGS", 7500.00, date(2023, 3, 15)),
        (1004, 1, 101, "ACC001004", "CHECKING", 2000.00, date(2023, 4, 10))
    ]
    
    # Transaction sample data
    transaction_schema = StructType([
        StructField("TRANSACTION_ID", IntegerType(), True),
        StructField("ACCOUNT_ID", IntegerType(), True),
        StructField("TRANSACTION_TYPE", StringType(), True),
        StructField("AMOUNT", DoubleType(), True),
        StructField("TRANSACTION_DATE", DateType(), True),
        StructField("DESCRIPTION", StringType(), True)
    ])
    
    transaction_data = [
        (10001, 1001, "DEPOSIT", 1000.00, date(2023, 5, 1), "Salary deposit"),
        (10002, 1001, "WITHDRAWAL", 200.00, date(2023, 5, 2), "ATM withdrawal"),
        (10003, 1002, "DEPOSIT", 500.00, date(2023, 5, 3), "Check deposit"),
        (10004, 1003, "TRANSFER", 1500.00, date(2023, 5, 4), "Wire transfer"),
        (10005, 1004, "DEPOSIT", 300.00, date(2023, 5, 5), "Cash deposit")
    ]
    
    # [ADDED] - Branch Operational Details sample data (New source table)
    branch_operational_schema = StructType([
        StructField("BRANCH_ID", IntegerType(), True),
        StructField("REGION", StringType(), True),
        StructField("MANAGER_NAME", StringType(), True),
        StructField("LAST_AUDIT_DATE", DateType(), True),
        StructField("IS_ACTIVE", StringType(), True)
    ])
    
    branch_operational_data = [
        (101, "Northeast", "Alice Manager", date(2023, 4, 15), "Y"),
        (102, "West Coast", "Bob Manager", date(2023, 4, 20), "Y"),
        (103, "Midwest", "Charlie Manager", date(2023, 3, 30), "N")  # Inactive branch
    ]
    
    # Create DataFrames
    customer_df = spark.createDataFrame(customer_data, customer_schema)
    branch_df = spark.createDataFrame(branch_data, branch_schema)
    account_df = spark.createDataFrame(account_data, account_schema)
    transaction_df = spark.createDataFrame(transaction_data, transaction_schema)
    branch_operational_df = spark.createDataFrame(branch_operational_data, branch_operational_schema)
    
    return customer_df, branch_df, account_df, transaction_df, branch_operational_df