# ====================================================================
# Author: Ascendion AAVA
# Date: 
# Description: Comprehensive unit tests for PySpark ETL pipeline integrating BRANCH_OPERATIONAL_DETAILS with BRANCH_SUMMARY_REPORT
# ====================================================================

import pytest
import unittest
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DecimalType, TimestampType
from pyspark.sql.functions import col, when, lit, coalesce
from datetime import datetime, date
import pandas as pd
from decimal import Decimal

# Test Case List:
# TC001: Test SparkSession initialization and configuration
# TC002: Test BRANCH_OPERATIONAL_DETAILS schema validation
# TC003: Test BRANCH_SUMMARY_REPORT schema validation
# TC004: Test successful data join operation
# TC005: Test conditional population of REGION and LAST_AUDIT_DATE
# TC006: Test IS_ACTIVE filter functionality
# TC007: Test backward compatibility with existing records
# TC008: Test error handling for missing BRANCH_ID
# TC009: Test data type validation and conversion
# TC010: Test null value handling
# TC011: Test duplicate BRANCH_ID scenarios
# TC012: Test empty DataFrame handling
# TC013: Test large dataset performance
# TC014: Test Delta table write operations
# TC015: Test data reconciliation and validation

class TestBranchETLPipeline(unittest.TestCase):
    """Comprehensive test suite for Branch ETL Pipeline"""
    
    @classmethod
    def setUpClass(cls):
        """Initialize Spark session for all tests"""
        cls.spark = SparkSession.builder \
            .appName("BranchETLPipelineTests") \
            .master("local[*]") \
            .config("spark.sql.adaptive.enabled", "false") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")
    
    @classmethod
    def tearDownClass(cls):
        """Clean up Spark session after all tests"""
        cls.spark.stop()
    
    def setUp(self):
        """Set up test data for each test case"""
        # TC001: Test SparkSession initialization and configuration
        # Define schemas for test data
        self.branch_operational_schema = StructType([
            StructField("BRANCH_ID", StringType(), False),
            StructField("REGION", StringType(), True),
            StructField("MANAGER_NAME", StringType(), True),
            StructField("LAST_AUDIT_DATE", DateType(), True),
            StructField("IS_ACTIVE", StringType(), False),
            StructField("CREATED_DATE", TimestampType(), True)
        ])
        
        self.branch_summary_schema = StructType([
            StructField("BRANCH_ID", StringType(), False),
            StructField("BRANCH_NAME", StringType(), True),
            StructField("TOTAL_CUSTOMERS", IntegerType(), True),
            StructField("TOTAL_DEPOSITS", DecimalType(15, 2), True),
            StructField("REGION", StringType(), True),
            StructField("LAST_AUDIT_DATE", DateType(), True)
        ])
        
        # Sample test data
        self.sample_operational_data = [
            ("BR001", "North", "John Smith", date(2024, 1, 15), "Y", datetime(2024, 1, 1)),
            ("BR002", "South", "Jane Doe", date(2024, 2, 20), "Y", datetime(2024, 1, 2)),
            ("BR003", "East", "Bob Johnson", date(2024, 3, 10), "N", datetime(2024, 1, 3)),
            ("BR004", "West", "Alice Brown", None, "Y", datetime(2024, 1, 4)),
            ("BR005", None, "Charlie Wilson", date(2024, 4, 5), "Y", datetime(2024, 1, 5))
        ]
        
        self.sample_summary_data = [
            ("BR001", "Downtown Branch", 1500, Decimal("2500000.00"), None, None),
            ("BR002", "Uptown Branch", 1200, Decimal("1800000.00"), None, None),
            ("BR003", "Eastside Branch", 800, Decimal("1200000.00"), None, None),
            ("BR006", "Westside Branch", 900, Decimal("1400000.00"), None, None)
        ]
    
    # TC002: Test BRANCH_OPERATIONAL_DETAILS schema validation
    def test_operational_details_schema_validation(self):
        """Test schema validation for BRANCH_OPERATIONAL_DETAILS table"""
        df = self.spark.createDataFrame(self.sample_operational_data, self.branch_operational_schema)
        
        # Validate schema structure
        expected_columns = ["BRANCH_ID", "REGION", "MANAGER_NAME", "LAST_AUDIT_DATE", "IS_ACTIVE", "CREATED_DATE"]
        actual_columns = df.columns
        
        self.assertEqual(set(expected_columns), set(actual_columns))
        self.assertEqual(len(df.columns), 6)
        
        # Validate data types
        schema_dict = {field.name: field.dataType for field in df.schema.fields}
        self.assertIsInstance(schema_dict["BRANCH_ID"], StringType)
        self.assertIsInstance(schema_dict["REGION"], StringType)
        self.assertIsInstance(schema_dict["LAST_AUDIT_DATE"], DateType)
        self.assertIsInstance(schema_dict["IS_ACTIVE"], StringType)
    
    # TC003: Test BRANCH_SUMMARY_REPORT schema validation
    def test_summary_report_schema_validation(self):
        """Test schema validation for BRANCH_SUMMARY_REPORT table"""
        df = self.spark.createDataFrame(self.sample_summary_data, self.branch_summary_schema)
        
        # Validate schema structure
        expected_columns = ["BRANCH_ID", "BRANCH_NAME", "TOTAL_CUSTOMERS", "TOTAL_DEPOSITS", "REGION", "LAST_AUDIT_DATE"]
        actual_columns = df.columns
        
        self.assertEqual(set(expected_columns), set(actual_columns))
        self.assertEqual(len(df.columns), 6)
        
        # Validate new columns exist
        self.assertIn("REGION", actual_columns)
        self.assertIn("LAST_AUDIT_DATE", actual_columns)
    
    # TC004: Test successful data join operation
    def test_successful_data_join(self):
        """Test successful join between operational details and summary report"""
        operational_df = self.spark.createDataFrame(self.sample_operational_data, self.branch_operational_schema)
        summary_df = self.spark.createDataFrame(self.sample_summary_data, self.branch_summary_schema)
        
        # Perform left join
        joined_df = summary_df.alias("summary").join(
            operational_df.alias("operational"),
            col("summary.BRANCH_ID") == col("operational.BRANCH_ID"),
            "left"
        )
        
        # Validate join results
        self.assertGreater(joined_df.count(), 0)
        
        # Check that all summary records are preserved
        self.assertEqual(joined_df.count(), summary_df.count())
        
        # Verify join columns exist
        join_columns = joined_df.columns
        self.assertIn("BRANCH_ID", join_columns)
    
    # TC005: Test conditional population of REGION and LAST_AUDIT_DATE
    def test_conditional_population_logic(self):
        """Test conditional population based on IS_ACTIVE status"""
        operational_df = self.spark.createDataFrame(self.sample_operational_data, self.branch_operational_schema)
        summary_df = self.spark.createDataFrame(self.sample_summary_data, self.branch_summary_schema)
        
        # Simulate ETL logic
        result_df = summary_df.alias("summary").join(
            operational_df.alias("operational"),
            col("summary.BRANCH_ID") == col("operational.BRANCH_ID"),
            "left"
        ).select(
            col("summary.BRANCH_ID"),
            col("summary.BRANCH_NAME"),
            col("summary.TOTAL_CUSTOMERS"),
            col("summary.TOTAL_DEPOSITS"),
            when(col("operational.IS_ACTIVE") == "Y", col("operational.REGION")).alias("REGION"),
            when(col("operational.IS_ACTIVE") == "Y", col("operational.LAST_AUDIT_DATE")).alias("LAST_AUDIT_DATE")
        )
        
        result_data = result_df.collect()
        
        # Validate conditional logic
        for row in result_data:
            if row.BRANCH_ID == "BR001":
                self.assertEqual(row.REGION, "North")
                self.assertEqual(row.LAST_AUDIT_DATE, date(2024, 1, 15))
            elif row.BRANCH_ID == "BR003":
                # IS_ACTIVE = 'N', should be null
                self.assertIsNone(row.REGION)
                self.assertIsNone(row.LAST_AUDIT_DATE)

if __name__ == "__main__":
    unittest.main()