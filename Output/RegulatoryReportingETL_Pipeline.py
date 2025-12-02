# ====================================================================
# Author: Ascendion AAVA
# Date: <Leave it blank>
# Description: Enhanced PySpark ETL pipeline integrating BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT
# ====================================================================

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum, when, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, DateType
from datetime import date

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_spark_session(app_name: str = "RegulatoryReportingETL") -> SparkSession:
    """
    # [MODIFIED] - Updated to use getActiveSession() for Spark Connect compatibility
    Initializes and returns a Spark session compatible with Spark Connect.
    """
    try:
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

def create_sample_data(spark: SparkSession) -> dict:
    """
    # [ADDED] - Creates sample data for all source tables to make the script self-contained.
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
        (1, "John Doe", "john.doe@email.com", "123-456-7890", "123 Main St", date(2023, 1, 15)),
        (2, "Jane Smith", "jane.smith@email.com", "098-765-4321", "456 Oak Ave", date(2023, 2, 20)),
        (3, "Bob Johnson", "bob.johnson@email.com", "555-123-4567", "789 Pine Rd", date(2023, 3, 10))
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
        (101, "Downtown Branch", "DT001", "New York", "NY", "USA"),
        (102, "Uptown Branch", "UT002", "Los Angeles", "CA", "USA"),
        (103, "Suburban Branch", "SB003", "Chicago", "IL", "USA")
    ]
    
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
        (1001, 1, 101, "ACC001001", "SAVINGS", 15000.00, date(2023, 1, 20)),
        (1002, 2, 102, "ACC002002", "CHECKING", 25000.00, date(2023, 2, 25)),
        (1003, 3, 103, "ACC003003", "SAVINGS", 35000.00, date(2023, 3, 15))
    ]
    
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
        (2001, 1001, "DEPOSIT", 5000.00, date(2023, 4, 1), "Salary deposit"),
        (2002, 1001, "WITHDRAWAL", 1000.00, date(2023, 4, 5), "ATM withdrawal"),
        (2003, 1002, "DEPOSIT", 10000.00, date(2023, 4, 2), "Business deposit"),
        (2004, 1002, "TRANSFER", 2000.00, date(2023, 4, 6), "Transfer to savings"),
        (2005, 1003, "DEPOSIT", 7500.00, date(2023, 4, 3), "Investment return"),
        (2006, 1003, "WITHDRAWAL", 500.00, date(2023, 4, 7), "Cash withdrawal")
    ]
    
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
        (102, "West Coast", "Bob Manager", date(2023, 3, 20), "Y"),
        (103, "Midwest", "Charlie Manager", date(2023, 2, 28), "N")  # Inactive branch
    ]
    
    # Create DataFrames
    sample_data = {
        "customer": spark.createDataFrame(customer_data, customer_schema),
        "branch": spark.createDataFrame(branch_data, branch_schema),
        "account": spark.createDataFrame(account_data, account_schema),
        "transaction": spark.createDataFrame(transaction_data, transaction_schema),
        "branch_operational": spark.createDataFrame(branch_operational_data, branch_operational_schema)  # [ADDED]
    }
    
    return sample_data