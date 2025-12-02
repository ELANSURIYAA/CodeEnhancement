====================================================================
# Author: Ascendion AAVA
# Date: <Leave it blank>
# Description: Enhanced PySpark ETL pipeline integrating BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT
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
        logger.info("Spark session created successfully.")
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark session: {e}")
        raise

def create_sample_data(spark: SparkSession) -> tuple:
    """
    Creates sample DataFrames for testing the ETL pipeline.
    [ADDED] - New function to create self-contained sample data
    """
    logger.info("Creating sample data for testing")
    
    # Sample Customer data
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
    
    # Sample Branch data
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
    
    # Sample Account data
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
        (1001, 1, 101, "ACC001", "SAVINGS", 5000.00, date(2023, 1, 16)),
        (1002, 2, 102, "ACC002", "CHECKING", 3000.00, date(2023, 2, 21)),
        (1003, 3, 103, "ACC003", "SAVINGS", 7500.00, date(2023, 3, 11)),
        (1004, 1, 102, "ACC004", "CHECKING", 2500.00, date(2023, 4, 5))
    ]
    
    account_df = spark.createDataFrame(account_data, account_schema)
    
    # Sample Transaction data
    transaction_schema = StructType([
        StructField("TRANSACTION_ID", IntegerType(), True),
        StructField("ACCOUNT_ID", IntegerType(), True),
        StructField("TRANSACTION_TYPE", StringType(), True),
        StructField("AMOUNT", DoubleType(), True),
        StructField("TRANSACTION_DATE", DateType(), True),
        StructField("DESCRIPTION", StringType(), True)
    ])
    
    transaction_data = [
        (2001, 1001, "DEPOSIT", 1000.00, date(2023, 5, 1), "Salary deposit"),
        (2002, 1001, "WITHDRAWAL", 500.00, date(2023, 5, 2), "ATM withdrawal"),
        (2003, 1002, "DEPOSIT", 2000.00, date(2023, 5, 1), "Check deposit"),
        (2004, 1003, "TRANSFER", 1500.00, date(2023, 5, 3), "Online transfer"),
        (2005, 1004, "DEPOSIT", 800.00, date(2023, 5, 4), "Cash deposit"),
        (2006, 1002, "WITHDRAWAL", 300.00, date(2023, 5, 5), "Debit card purchase")
    ]
    
    transaction_df = spark.createDataFrame(transaction_data, transaction_schema)
    
    # [ADDED] - Sample Branch Operational Details data (new source table)
    branch_operational_schema = StructType([
        StructField("BRANCH_ID", IntegerType(), True),
        StructField("REGION", StringType(), True),
        StructField("MANAGER_NAME", StringType(), True),
        StructField("LAST_AUDIT_DATE", DateType(), True),
        StructField("IS_ACTIVE", StringType(), True)
    ])
    
    branch_operational_data = [
        (101, "Northeast", "Alice Johnson", date(2023, 4, 15), "Y"),
        (102, "West Coast", "Bob Wilson", date(2023, 4, 20), "Y"),
        (103, "Midwest", "Carol Davis", date(2023, 3, 10), "N")  # Inactive branch
    ]
    
    branch_operational_df = spark.createDataFrame(branch_operational_data, branch_operational_schema)
    
    return customer_df, account_df, transaction_df, branch_df, branch_operational_df