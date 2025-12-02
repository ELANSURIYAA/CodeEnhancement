# ====================================================================
# Author: Ascendion AAVA
# Date: <Leave it blank>
# Description: Python-based test script for RegulatoryReportingETL Pipeline (without PyTest framework)
# ====================================================================

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum, when, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, DateType
from datetime import date
import tempfile
import shutil
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TestRegulatoryReportingETL:
    """
    Test class for Regulatory Reporting ETL without using PyTest framework.
    """
    
    def __init__(self):
        self.spark = None
        self.test_results = []
        self.temp_dir = tempfile.mkdtemp()
        
    def setup_spark(self):
        """Setup Spark session for testing."""
        try:
            self.spark = SparkSession.getActiveSession()
            if self.spark is None:
                self.spark = SparkSession.builder \
                    .appName("TestRegulatoryReportingETL") \
                    .master("local[*]") \
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                    .getOrCreate()
            logger.info("Test Spark session created successfully.")
        except Exception as e:
            logger.error(f"Error creating test Spark session: {e}")
            raise
    
    def teardown_spark(self):
        """Cleanup Spark session and temporary files."""
        if self.spark:
            self.spark.stop()
            logger.info("Test Spark session stopped.")
        
        # Cleanup temporary directory
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
            logger.info("Temporary files cleaned up.")
    
    def create_test_data_scenario1(self):
        """Create test data for Scenario 1: Insert new records."""
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
            (201, "New Downtown Branch", "NDT001", "Boston", "MA", "USA"),
            (202, "New Uptown Branch", "NUT002", "Seattle", "WA", "USA")
        ]
        
        branch_df = self.spark.createDataFrame(branch_data, branch_schema)
        
        # Account data
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
            (2001, 1, 201, "ACC201001", "SAVINGS", 8000.00, date(2023, 6, 1)),
            (2002, 2, 202, "ACC202002", "CHECKING", 4500.00, date(2023, 6, 2))
        ]
        
        account_df = self.spark.createDataFrame(account_data, account_schema)
        
        # Transaction data
        transaction_schema = StructType([
            StructField("TRANSACTION_ID", IntegerType(), True),
            StructField("ACCOUNT_ID", IntegerType(), True),
            StructField("TRANSACTION_TYPE", StringType(), True),
            StructField("AMOUNT", DecimalType(15, 2), True),
            StructField("TRANSACTION_DATE", DateType(), True),
            StructField("DESCRIPTION", StringType(), True)
        ])
        
        transaction_data = [
            (20001, 2001, "DEPOSIT", 2000.00, date(2023, 6, 5), "Initial deposit"),
            (20002, 2001, "WITHDRAWAL", 500.00, date(2023, 6, 6), "ATM withdrawal"),
            (20003, 2002, "DEPOSIT", 1000.00, date(2023, 6, 5), "Payroll deposit")
        ]
        
        transaction_df = self.spark.createDataFrame(transaction_data, transaction_schema)
        
        # Branch Operational Details
        branch_operational_schema = StructType([
            StructField("BRANCH_ID", IntegerType(), True),
            StructField("REGION", StringType(), True),
            StructField("MANAGER_NAME", StringType(), True),
            StructField("LAST_AUDIT_DATE", DateType(), True),
            StructField("IS_ACTIVE", StringType(), True)
        ])
        
        branch_operational_data = [
            (201, "Northeast", "David Smith", date(2023, 5, 15), "Y"),
            (202, "Northwest", "Emma Wilson", date(2023, 5, 20), "Y")
        ]
        
        branch_operational_df = self.spark.createDataFrame(branch_operational_data, branch_operational_schema)
        
        return transaction_df, account_df, branch_df, branch_operational_df
    
    def create_test_data_scenario2(self):
        """Create test data for Scenario 2: Update existing records."""
        # Existing branch data (same IDs as scenario 1 but updated values)
        branch_schema = StructType([
            StructField("BRANCH_ID", IntegerType(), True),
            StructField("BRANCH_NAME", StringType(), True),
            StructField("BRANCH_CODE", StringType(), True),
            StructField("CITY", StringType(), True),
            StructField("STATE", StringType(), True),
            StructField("COUNTRY", StringType(), True)
        ])
        
        branch_data = [
            (201, "Updated Downtown Branch", "UDT001", "Boston", "MA", "USA"),
            (202, "Updated Uptown Branch", "UUT002", "Seattle", "WA", "USA")
        ]
        
        branch_df = self.spark.createDataFrame(branch_data, branch_schema)
        
        # Updated account data
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
            (2001, 1, 201, "ACC201001", "SAVINGS", 9000.00, date(2023, 6, 1)),
            (2002, 2, 202, "ACC202002", "CHECKING", 5500.00, date(2023, 6, 2))
        ]
        
        account_df = self.spark.createDataFrame(account_data, account_schema)
        
        # Additional transactions
        transaction_schema = StructType([
            StructField("TRANSACTION_ID", IntegerType(), True),
            StructField("ACCOUNT_ID", IntegerType(), True),
            StructField("TRANSACTION_TYPE", StringType(), True),
            StructField("AMOUNT", DecimalType(15, 2), True),
            StructField("TRANSACTION_DATE", DateType(), True),
            StructField("DESCRIPTION", StringType(), True)
        ])
        
        transaction_data = [
            (20004, 2001, "DEPOSIT", 3000.00, date(2023, 6, 10), "Bonus deposit"),
            (20005, 2002, "TRANSFER", 2000.00, date(2023, 6, 11), "Internal transfer")
        ]
        
        transaction_df = self.spark.createDataFrame(transaction_data, transaction_schema)
        
        # Updated Branch Operational Details
        branch_operational_schema = StructType([
            StructField("BRANCH_ID", IntegerType(), True),
            StructField("REGION", StringType(), True),
            StructField("MANAGER_NAME", StringType(), True),
            StructField("LAST_AUDIT_DATE", DateType(), True),
            StructField("IS_ACTIVE", StringType(), True)
        ])
        
        branch_operational_data = [
            (201, "Northeast", "David Smith Jr.", date(2023, 6, 15), "Y"),
            (202, "Northwest", "Emma Wilson Sr.", date(2023, 6, 20), "Y")
        ]
        
        branch_operational_df = self.spark.createDataFrame(branch_operational_data, branch_operational_schema)
        
        return transaction_df, account_df, branch_df, branch_operational_df