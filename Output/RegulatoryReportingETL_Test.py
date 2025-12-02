====================================================================
Author: Ascendion AAVA
Date: <Leave it blank>
Description: Python-based test script for testing PySpark ETL pipeline with insert and update scenarios
====================================================================

import sys
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum as spark_sum, when, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from datetime import date
import tempfile
import shutil
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TestResult:
    """
    Class to store test results for reporting
    """
    def __init__(self, scenario_name: str):
        self.scenario_name = scenario_name
        self.input_data = []
        self.output_data = []
        self.status = "FAIL"
        self.details = ""

def get_spark_session(app_name: str = "RegulatoryReportingETL_Test") -> SparkSession:
    """
    Initializes and returns a Spark session for testing.
    """
    try:
        try:
            spark = SparkSession.getActiveSession()
            if spark is None:
                spark = SparkSession.builder.appName(app_name).getOrCreate()
        except:
            spark = SparkSession.builder.appName(app_name).getOrCreate()
        
        spark.conf.set("spark.sql.adaptive.enabled", "true")
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        logger.info("Test Spark session created successfully.")
        return spark
    except Exception as e:
        logger.error(f"Error creating test Spark session: {e}")
        raise

def create_base_data(spark: SparkSession) -> tuple:
    """
    Creates base data that exists in the target table before testing.
    """
    logger.info("Creating base data for testing")
    
    # Base branch summary data (existing records)
    base_branch_summary_schema = StructType([
        StructField("BRANCH_ID", IntegerType(), True),
        StructField("BRANCH_NAME", StringType(), True),
        StructField("TOTAL_TRANSACTIONS", IntegerType(), True),
        StructField("TOTAL_AMOUNT", DoubleType(), True),
        StructField("REGION", StringType(), True),
        StructField("LAST_AUDIT_DATE", DateType(), True)
    ])
    
    base_branch_summary_data = [
        (101, "Downtown Branch", 10, 5000.00, "Northeast", date(2023, 4, 15)),
        (102, "Uptown Branch", 8, 3000.00, "West Coast", date(2023, 4, 20))
    ]
    
    base_df = spark.createDataFrame(base_branch_summary_data, base_branch_summary_schema)
    return base_df

def create_test_scenario_1_data(spark: SparkSession) -> tuple:
    """
    Creates test data for Scenario 1: Insert new records
    """
    logger.info("Creating test data for Scenario 1: Insert")
    
    # Branch data
    branch_schema = StructType([
        StructField("BRANCH_ID", IntegerType(), True),
        StructField("BRANCH_NAME", StringType(), True),
        StructField("BRANCH_CODE", StringType(), True),
        StructField("CITY", StringType(), True),
        StructField("STATE", StringType(), True),
        StructField("COUNTRY", StringType(), True)
    ])
    
    branch_data = [
        (103, "Central Branch", "CTR003", "Chicago", "IL", "USA"),  # New branch
        (104, "South Branch", "STH004", "Miami", "FL", "USA")       # New branch
    ]
    
    # Account data
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
        (1003, 3, 103, "ACC003003", "SAVINGS", 7500.00, date(2023, 3, 15)),
        (1004, 4, 104, "ACC004004", "CHECKING", 2000.00, date(2023, 4, 10))
    ]
    
    # Transaction data
    transaction_schema = StructType([
        StructField("TRANSACTION_ID", IntegerType(), True),
        StructField("ACCOUNT_ID", IntegerType(), True),
        StructField("TRANSACTION_TYPE", StringType(), True),
        StructField("AMOUNT", DoubleType(), True),
        StructField("TRANSACTION_DATE", DateType(), True),
        StructField("DESCRIPTION", StringType(), True)
    ])
    
    transaction_data = [
        (10003, 1003, "TRANSFER", 1500.00, date(2023, 5, 4), "Wire transfer"),
        (10004, 1003, "DEPOSIT", 500.00, date(2023, 5, 5), "Cash deposit"),
        (10005, 1004, "DEPOSIT", 300.00, date(2023, 5, 6), "Check deposit")
    ]
    
    # Branch Operational Details
    branch_operational_schema = StructType([
        StructField("BRANCH_ID", IntegerType(), True),
        StructField("REGION", StringType(), True),
        StructField("MANAGER_NAME", StringType(), True),
        StructField("LAST_AUDIT_DATE", DateType(), True),
        StructField("IS_ACTIVE", StringType(), True)
    ])
    
    branch_operational_data = [
        (103, "Midwest", "Charlie Manager", date(2023, 3, 30), "Y"),  # Active
        (104, "Southeast", "David Manager", date(2023, 4, 25), "Y")   # Active
    ]
    
    # Create DataFrames
    branch_df = spark.createDataFrame(branch_data, branch_schema)
    account_df = spark.createDataFrame(account_data, account_schema)
    transaction_df = spark.createDataFrame(transaction_data, transaction_schema)
    branch_operational_df = spark.createDataFrame(branch_operational_data, branch_operational_schema)
    
    return branch_df, account_df, transaction_df, branch_operational_df

def create_test_scenario_2_data(spark: SparkSession) -> tuple:
    """
    Creates test data for Scenario 2: Update existing records
    """
    logger.info("Creating test data for Scenario 2: Update")
    
    # Branch data (existing branches)
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
        (102, "Uptown Branch", "UPT002", "Los Angeles", "CA", "USA")
    ]
    
    # Account data
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
        (1002, 2, 102, "ACC002002", "CHECKING", 3000.00, date(2023, 2, 25))
    ]
    
    # Transaction data (additional transactions for existing branches)
    transaction_schema = StructType([
        StructField("TRANSACTION_ID", IntegerType(), True),
        StructField("ACCOUNT_ID", IntegerType(), True),
        StructField("TRANSACTION_TYPE", StringType(), True),
        StructField("AMOUNT", DoubleType(), True),
        StructField("TRANSACTION_DATE", DateType(), True),
        StructField("DESCRIPTION", StringType(), True)
    ])
    
    transaction_data = [
        (10001, 1001, "DEPOSIT", 2000.00, date(2023, 5, 1), "Large deposit"),
        (10002, 1001, "WITHDRAWAL", 500.00, date(2023, 5, 2), "ATM withdrawal"),
        (10003, 1002, "DEPOSIT", 1000.00, date(2023, 5, 3), "Check deposit"),
        (10004, 1002, "TRANSFER", 750.00, date(2023, 5, 4), "Wire transfer")
    ]
    
    # Branch Operational Details (updated information)
    branch_operational_schema = StructType([
        StructField("BRANCH_ID", IntegerType(), True),
        StructField("REGION", StringType(), True),
        StructField("MANAGER_NAME", StringType(), True),
        StructField("LAST_AUDIT_DATE", DateType(), True),
        StructField("IS_ACTIVE", StringType(), True)
    ])
    
    branch_operational_data = [
        (101, "Northeast Updated", "Alice Manager Updated", date(2023, 5, 15), "Y"),
        (102, "West Coast Updated", "Bob Manager Updated", date(2023, 5, 20), "Y")
    ]
    
    # Create DataFrames
    branch_df = spark.createDataFrame(branch_data, branch_schema)
    account_df = spark.createDataFrame(account_data, account_schema)
    transaction_df = spark.createDataFrame(transaction_data, transaction_schema)
    branch_operational_df = spark.createDataFrame(branch_operational_data, branch_operational_schema)
    
    return branch_df, account_df, transaction_df, branch_operational_df