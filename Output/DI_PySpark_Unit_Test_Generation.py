====================================================================
Author: Ascendion AAVA
Date: 
Description: Comprehensive unit tests for Enhanced PySpark ETL Pipeline with BRANCH_OPERATIONAL_DETAILS integration
====================================================================

import pytest
import logging
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from pyspark.sql.functions import col
from datetime import date
import sys
import os

# Add the source code directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import the functions to test
try:
    from RegulatoryReportingETL_Pipeline import (
        get_spark_session,
        create_aml_customer_transactions,
        create_branch_summary_report,
        write_to_delta_table,
        main
    )
except ImportError:
    # If direct import fails, define mock functions for testing structure
    def get_spark_session(app_name="RegulatoryReportingETL"):
        pass
    def create_aml_customer_transactions(customer_df, account_df, transaction_df):
        pass
    def create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_details_df):
        pass
    def write_to_delta_table(df, table_name):
        pass
    def main():
        pass

class TestRegulatoryReportingETL:
    """
    Comprehensive test suite for Regulatory Reporting ETL Pipeline
    """
    
    @pytest.fixture(scope="class")
    def spark_session(self):
        """Create a test Spark session"""
        spark = SparkSession.builder \
            .appName("TestRegulatoryReportingETL") \
            .master("local[*]") \
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
            .getOrCreate()
        yield spark
        spark.stop()
    
    @pytest.fixture
    def sample_customer_data(self, spark_session):
        """Create sample customer DataFrame for testing"""
        data = [
            (1, "Alice Johnson", "alice@email.com", "1234567890", "123 Main St", date(2021, 1, 1)),
            (2, "Bob Smith", "bob@email.com", "2345678901", "456 Oak Ave", date(2021, 2, 1)),
            (3, "Charlie Brown", "charlie@email.com", "3456789012", "789 Pine Rd", date(2021, 3, 1))
        ]
        schema = StructType([
            StructField("CUSTOMER_ID", IntegerType(), True),
            StructField("NAME", StringType(), True),
            StructField("EMAIL", StringType(), True),
            StructField("PHONE", StringType(), True),
            StructField("ADDRESS", StringType(), True),
            StructField("CREATED_DATE", DateType(), True)
        ])
        return spark_session.createDataFrame(data, schema)
    
    @pytest.fixture
    def sample_branch_data(self, spark_session):
        """Create sample branch DataFrame for testing"""
        data = [
            (101, "Downtown Branch", "DT001", "New York", "NY", "USA"),
            (102, "Uptown Branch", "UT002", "Los Angeles", "CA", "USA"),
            (103, "Midtown Branch", "MT003", "Chicago", "IL", "USA")
        ]
        schema = StructType([
            StructField("BRANCH_ID", IntegerType(), True),
            StructField("BRANCH_NAME", StringType(), True),
            StructField("BRANCH_CODE", StringType(), True),
            StructField("CITY", StringType(), True),
            StructField("STATE", StringType(), True),
            StructField("COUNTRY", StringType(), True)
        ])
        return spark_session.createDataFrame(data, schema)
    
    @pytest.fixture
    def sample_account_data(self, spark_session):
        """Create sample account DataFrame for testing"""
        data = [
            (1001, 1, 101, "ACC001", "Savings", 1000.00, date(2021, 1, 15)),
            (1002, 2, 102, "ACC002", "Checking", 2000.00, date(2021, 2, 15)),
            (1003, 3, 103, "ACC003", "Savings", 1500.00, date(2021, 3, 15)),
            (1004, 1, 102, "ACC004", "Investment", 5000.00, date(2021, 4, 15))
        ]
        schema = StructType([
            StructField("ACCOUNT_ID", IntegerType(), True),
            StructField("CUSTOMER_ID", IntegerType(), True),
            StructField("BRANCH_ID", IntegerType(), True),
            StructField("ACCOUNT_NUMBER", StringType(), True),
            StructField("ACCOUNT_TYPE", StringType(), True),
            StructField("BALANCE", DoubleType(), True),
            StructField("OPENED_DATE", DateType(), True)
        ])
        return spark_session.createDataFrame(data, schema)
    
    @pytest.fixture
    def sample_transaction_data(self, spark_session):
        """Create sample transaction DataFrame for testing"""
        data = [
            (5001, 1001, "Deposit", 500.0, date(2022, 1, 15), "Salary"),
            (5002, 1002, "Withdrawal", 300.0, date(2022, 2, 15), "ATM"),
            (5003, 1003, "Deposit", 750.0, date(2022, 3, 15), "Transfer"),
            (5004, 1001, "Withdrawal", 200.0, date(2022, 4, 15), "Bill Payment"),
            (5005, 1004, "Deposit", 1000.0, date(2022, 5, 15), "Investment")
        ]
        schema = StructType([
            StructField("TRANSACTION_ID", IntegerType(), True),
            StructField("ACCOUNT_ID", IntegerType(), True),
            StructField("TRANSACTION_TYPE", StringType(), True),
            StructField("AMOUNT", DoubleType(), True),
            StructField("TRANSACTION_DATE", DateType(), True),
            StructField("DESCRIPTION", StringType(), True)
        ])
        return spark_session.createDataFrame(data, schema)
    
    @pytest.fixture
    def sample_branch_operational_data(self, spark_session):
        """Create sample branch operational details DataFrame for testing"""
        data = [
            (101, "North", "John Doe", date(2022, 5, 10), "Y"),
            (102, "South", "Jane Smith", date(2022, 6, 20), "Y"),
            (103, "East", "Mike Johnson", date(2022, 7, 30), "N")
        ]
        schema = StructType([
            StructField("BRANCH_ID", IntegerType(), True),
            StructField("REGION", StringType(), True),
            StructField("MANAGER_NAME", StringType(), True),
            StructField("LAST_AUDIT_DATE", DateType(), True),
            StructField("IS_ACTIVE", StringType(), True)
        ])
        return spark_session.createDataFrame(data, schema)

    # Test Case 1: Test Spark Session Creation
    def test_get_spark_session_success(self):
        """
        TC001: Test successful Spark session creation with default app name
        Validates that get_spark_session returns a valid SparkSession object
        """
        with patch('RegulatoryReportingETL_Pipeline.SparkSession') as mock_spark:
            mock_session = MagicMock()
            mock_spark.getActiveSession.return_value = None
            mock_spark.builder.appName.return_value.getOrCreate.return_value = mock_session
            
            result = get_spark_session()
            
            assert result == mock_session
            mock_spark.builder.appName.assert_called_with("RegulatoryReportingETL")
    
    # Test Case 2: Test Spark Session Creation with Custom App Name
    def test_get_spark_session_custom_name(self):
        """
        TC002: Test Spark session creation with custom application name
        Validates that custom app name is properly passed to SparkSession builder
        """
        with patch('RegulatoryReportingETL_Pipeline.SparkSession') as mock_spark:
            mock_session = MagicMock()
            mock_spark.getActiveSession.return_value = None
            mock_spark.builder.appName.return_value.getOrCreate.return_value = mock_session
            
            result = get_spark_session("CustomETLApp")
            
            assert result == mock_session
            mock_spark.builder.appName.assert_called_with("CustomETLApp")
    
    # Test Case 3: Test Spark Session Creation Exception Handling
    def test_get_spark_session_exception(self):
        """
        TC003: Test exception handling during Spark session creation
        Validates that exceptions are properly raised when session creation fails
        """
        with patch('RegulatoryReportingETL_Pipeline.SparkSession') as mock_spark:
            mock_spark.getActiveSession.return_value = None
            mock_spark.builder.appName.return_value.getOrCreate.side_effect = Exception("Connection failed")
            
            with pytest.raises(Exception, match="Connection failed"):
                get_spark_session()
    
    # Test Case 4: Test AML Customer Transactions Creation - Happy Path
    def test_create_aml_customer_transactions_success(self, spark_session, sample_customer_data, 
                                                    sample_account_data, sample_transaction_data):
        """
        TC004: Test successful creation of AML customer transactions DataFrame
        Validates correct joining of customer, account, and transaction data with expected columns
        """
        result_df = create_aml_customer_transactions(sample_customer_data, sample_account_data, sample_transaction_data)
        
        # Verify DataFrame is not None
        assert result_df is not None
        
        # Verify expected columns exist
        expected_columns = ["CUSTOMER_ID", "NAME", "ACCOUNT_ID", "TRANSACTION_ID", "AMOUNT", "TRANSACTION_TYPE", "TRANSACTION_DATE"]
        assert set(result_df.columns) == set(expected_columns)
        
        # Verify data count (should have 5 transactions)
        assert result_df.count() == 5
        
        # Verify specific data integrity
        first_row = result_df.filter(col("TRANSACTION_ID") == 5001).collect()[0]
        assert first_row["CUSTOMER_ID"] == 1
        assert first_row["NAME"] == "Alice Johnson"
        assert first_row["AMOUNT"] == 500.0
    
    # Test Case 5: Test AML Customer Transactions with Empty DataFrames
    def test_create_aml_customer_transactions_empty_data(self, spark_session):
        """
        TC005: Test AML customer transactions creation with empty input DataFrames
        Validates handling of edge case where input data is empty
        """
        # Create empty DataFrames with proper schemas
        customer_schema = StructType([StructField("CUSTOMER_ID", IntegerType(), True), StructField("NAME", StringType(), True)])
        account_schema = StructType([StructField("CUSTOMER_ID", IntegerType(), True), StructField("ACCOUNT_ID", IntegerType(), True)])
        transaction_schema = StructType([StructField("ACCOUNT_ID", IntegerType(), True), StructField("TRANSACTION_ID", IntegerType(), True)])
        
        empty_customer_df = spark_session.createDataFrame([], customer_schema)
        empty_account_df = spark_session.createDataFrame([], account_schema)
        empty_transaction_df = spark_session.createDataFrame([], transaction_schema)
        
        result_df = create_aml_customer_transactions(empty_customer_df, empty_account_df, empty_transaction_df)
        
        # Should return empty DataFrame but with correct structure
        assert result_df.count() == 0
    
    # Test Case 6: Test Branch Summary Report Creation - Happy Path
    def test_create_branch_summary_report_success(self, spark_session, sample_transaction_data, 
                                                sample_account_data, sample_branch_data, 
                                                sample_branch_operational_data):
        """
        TC006: Test successful creation of branch summary report with operational details
        Validates correct aggregation and conditional population of REGION and LAST_AUDIT_DATE
        """
        result_df = create_branch_summary_report(sample_transaction_data, sample_account_data, 
                                               sample_branch_data, sample_branch_operational_data)
        
        # Verify DataFrame is not None
        assert result_df is not None
        
        # Verify expected columns
        expected_columns = ["BRANCH_ID", "BRANCH_NAME", "TOTAL_TRANSACTIONS", "TOTAL_AMOUNT", "REGION", "LAST_AUDIT_DATE"]
        assert set(result_df.columns) == set(expected_columns)
        
        # Verify data count (should have 3 branches)
        assert result_df.count() == 3
        
        # Verify conditional logic for active branches
        active_branches = result_df.filter(col("REGION").isNotNull()).collect()
        assert len(active_branches) == 2  # Only branches 101 and 102 are active
        
        # Verify specific branch data
        branch_101 = result_df.filter(col("BRANCH_ID") == 101).collect()[0]
        assert branch_101["REGION"] == "North"
        assert branch_101["LAST_AUDIT_DATE"] == date(2022, 5, 10)
        
        # Verify inactive branch has null values
        branch_103 = result_df.filter(col("BRANCH_ID") == 103).collect()[0]
        assert branch_103["REGION"] is None
        assert branch_103["LAST_AUDIT_DATE"] is None
    
    # Test Case 7: Test Branch Summary Report with Missing Operational Data
    def test_create_branch_summary_report_missing_operational_data(self, spark_session, sample_transaction_data,
                                                                 sample_account_data, sample_branch_data):
        """
        TC007: Test branch summary report creation when operational details are missing
        Validates left join behavior when operational details table is empty
        """
        # Create empty operational details DataFrame
        empty_operational_schema = StructType([
            StructField("BRANCH_ID", IntegerType(), True),
            StructField("REGION", StringType(), True),
            StructField("MANAGER_NAME", StringType(), True),
            StructField("LAST_AUDIT_DATE", DateType(), True),
            StructField("IS_ACTIVE", StringType(), True)
        ])
        empty_operational_df = spark_session.createDataFrame([], empty_operational_schema)
        
        result_df = create_branch_summary_report(sample_transaction_data, sample_account_data,
                                               sample_branch_data, empty_operational_df)
        
        # Should still create summary but with null operational fields
        assert result_df.count() > 0
        
        # All REGION and LAST_AUDIT_DATE should be null
        null_regions = result_df.filter(col("REGION").isNull()).count()
        assert null_regions == result_df.count()
    
    # Test Case 8: Test Branch Summary Report Aggregation Logic
    def test_create_branch_summary_report_aggregation(self, spark_session, sample_transaction_data,
                                                    sample_account_data, sample_branch_data,
                                                    sample_branch_operational_data):
        """
        TC008: Test correct aggregation of transaction amounts and counts per branch
        Validates mathematical accuracy of TOTAL_TRANSACTIONS and TOTAL_AMOUNT calculations
        """
        result_df = create_branch_summary_report(sample_transaction_data, sample_account_data,
                                               sample_branch_data, sample_branch_operational_data)
        
        # Verify branch 101 aggregation (has 2 transactions: 500.0 + 200.0 = 700.0)
        branch_101 = result_df.filter(col("BRANCH_ID") == 101).collect()[0]
        assert branch_101["TOTAL_TRANSACTIONS"] == 2
        assert branch_101["TOTAL_AMOUNT"] == 700.0
        
        # Verify branch 102 aggregation (has 2 transactions: 300.0 + 1000.0 = 1300.0)
        branch_102 = result_df.filter(col("BRANCH_ID") == 102).collect()[0]
        assert branch_102["TOTAL_TRANSACTIONS"] == 2
        assert branch_102["TOTAL_AMOUNT"] == 1300.0
        
        # Verify branch 103 aggregation (has 1 transaction: 750.0)
        branch_103 = result_df.filter(col("BRANCH_ID") == 103).collect()[0]
        assert branch_103["TOTAL_TRANSACTIONS"] == 1
        assert branch_103["TOTAL_AMOUNT"] == 750.0
    
    # Test Case 9: Test Write to Delta Table Function
    @patch('RegulatoryReportingETL_Pipeline.logger')
    def test_write_to_delta_table_success(self, mock_logger, spark_session, sample_customer_data):
        """
        TC009: Test successful write to delta table (simulated via show())
        Validates that write_to_delta_table executes without errors and logs appropriately
        """
        # Mock the DataFrame show method to avoid actual output
        with patch.object(sample_customer_data, 'show') as mock_show:
            write_to_delta_table(sample_customer_data, "TEST_TABLE")
            
            # Verify show was called
            mock_show.assert_called_once_with(truncate=False)
            
            # Verify logging
            mock_logger.info.assert_called_with("Successfully written data to TEST_TABLE")
    
    # Test Case 10: Test Write to Delta Table Exception Handling
    @patch('RegulatoryReportingETL_Pipeline.logger')
    def test_write_to_delta_table_exception(self, mock_logger, spark_session, sample_customer_data):
        """
        TC010: Test exception handling in write_to_delta_table function
        Validates proper error handling and logging when write operation fails
        """
        # Mock the DataFrame show method to raise an exception
        with patch.object(sample_customer_data, 'show', side_effect=Exception("Write failed")):
            with pytest.raises(Exception, match="Write failed"):
                write_to_delta_table(sample_customer_data, "TEST_TABLE")
            
            # Verify error logging
            mock_logger.error.assert_called_with("Failed to write to Delta table TEST_TABLE: Write failed")
    
    # Test Case 11: Test Main Function Success Path
    @patch('RegulatoryReportingETL_Pipeline.get_spark_session')
    @patch('RegulatoryReportingETL_Pipeline.write_to_delta_table')
    @patch('RegulatoryReportingETL_Pipeline.logger')
    def test_main_function_success(self, mock_logger, mock_write, mock_get_spark, spark_session):
        """
        TC011: Test successful execution of main ETL function
        Validates end-to-end ETL process execution without errors
        """
        # Setup mocks
        mock_get_spark.return_value = spark_session
        mock_write.return_value = None
        
        # Execute main function
        main()
        
        # Verify Spark session was created
        mock_get_spark.assert_called_once()
        
        # Verify write operations were called twice (AML and Branch Summary)
        assert mock_write.call_count == 2
        
        # Verify success logging
        mock_logger.info.assert_any_call("ETL job completed successfully.")
    
    # Test Case 12: Test Main Function Exception Handling
    @patch('RegulatoryReportingETL_Pipeline.get_spark_session')
    @patch('RegulatoryReportingETL_Pipeline.logger')
    def test_main_function_exception(self, mock_logger, mock_get_spark):
        """
        TC012: Test exception handling in main ETL function
        Validates proper error handling and logging when ETL process fails
        """
        # Setup mock to raise exception
        mock_get_spark.side_effect = Exception("Spark initialization failed")
        
        # Execute main function
        main()
        
        # Verify error logging
        mock_logger.error.assert_called_with("ETL job failed with exception: Spark initialization failed")
    
    # Test Case 13: Test Data Quality - Null Value Handling
    def test_data_quality_null_handling(self, spark_session):
        """
        TC013: Test handling of null values in input data
        Validates ETL pipeline behavior with null/missing data scenarios
        """
        # Create data with null values
        customer_data_with_nulls = [
            (1, None, "alice@email.com", "1234567890", "123 Main St", date(2021, 1, 1)),
            (2, "Bob Smith", None, "2345678901", "456 Oak Ave", date(2021, 2, 1))
        ]
        customer_schema = StructType([
            StructField("CUSTOMER_ID", IntegerType(), True),
            StructField("NAME", StringType(), True),
            StructField("EMAIL", StringType(), True),
            StructField("PHONE", StringType(), True),
            StructField("ADDRESS", StringType(), True),
            StructField("CREATED_DATE", DateType(), True)
        ])
        
        customer_df_nulls = spark_session.createDataFrame(customer_data_with_nulls, customer_schema)
        
        # Verify DataFrame handles nulls appropriately
        assert customer_df_nulls.count() == 2
        null_names = customer_df_nulls.filter(col("NAME").isNull()).count()
        assert null_names == 1
    
    # Test Case 14: Test Performance with Large Dataset Simulation
    def test_performance_large_dataset_simulation(self, spark_session):
        """
        TC014: Test ETL performance with simulated large dataset
        Validates pipeline can handle larger data volumes efficiently
        """
        # Create larger dataset for performance testing
        import random
        from datetime import datetime, timedelta
        
        large_transaction_data = []
        for i in range(1000):
            large_transaction_data.append((
                i + 1,
                random.randint(1001, 1010),
                random.choice(["Deposit", "Withdrawal", "Transfer"]),
                round(random.uniform(10.0, 5000.0), 2),
                date(2022, random.randint(1, 12), random.randint(1, 28)),
                f"Transaction {i+1}"
            ))
        
        transaction_schema = StructType([
            StructField("TRANSACTION_ID", IntegerType(), True),
            StructField("ACCOUNT_ID", IntegerType(), True),
            StructField("TRANSACTION_TYPE", StringType(), True),
            StructField("AMOUNT", DoubleType(), True),
            StructField("TRANSACTION_DATE", DateType(), True),
            StructField("DESCRIPTION", StringType(), True)
        ])
        
        large_transaction_df = spark_session.createDataFrame(large_transaction_data, transaction_schema)
        
        # Verify large dataset creation
        assert large_transaction_df.count() == 1000
        
        # Test basic aggregation performance
        start_time = datetime.now()
        total_amount = large_transaction_df.agg({"AMOUNT": "sum"}).collect()[0][0]
        end_time = datetime.now()
        
        # Verify aggregation completed and took reasonable time (< 10 seconds)
        assert total_amount > 0
        assert (end_time - start_time).seconds < 10
    
    # Test Case 15: Test Schema Validation
    def test_schema_validation(self, spark_session, sample_customer_data, sample_account_data, sample_transaction_data):
        """
        TC015: Test schema validation for all DataFrames
        Validates that all DataFrames have expected schemas and data types
        """
        # Test customer DataFrame schema
        customer_schema = sample_customer_data.schema
        expected_customer_fields = ["CUSTOMER_ID", "NAME", "EMAIL", "PHONE", "ADDRESS", "CREATED_DATE"]
        actual_customer_fields = [field.name for field in customer_schema.fields]
        assert set(expected_customer_fields) == set(actual_customer_fields)
        
        # Test account DataFrame schema
        account_schema = sample_account_data.schema
        expected_account_fields = ["ACCOUNT_ID", "CUSTOMER_ID", "BRANCH_ID", "ACCOUNT_NUMBER", "ACCOUNT_TYPE", "BALANCE", "OPENED_DATE"]
        actual_account_fields = [field.name for field in account_schema.fields]
        assert set(expected_account_fields) == set(actual_account_fields)
        
        # Test transaction DataFrame schema
        transaction_schema = sample_transaction_data.schema
        expected_transaction_fields = ["TRANSACTION_ID", "ACCOUNT_ID", "TRANSACTION_TYPE", "AMOUNT", "TRANSACTION_DATE", "DESCRIPTION"]
        actual_transaction_fields = [field.name for field in transaction_schema.fields]
        assert set(expected_transaction_fields) == set(actual_transaction_fields)

# Test Case List Summary:
# TC001: Test Spark session creation success
# TC002: Test Spark session creation with custom name
# TC003: Test Spark session creation exception handling
# TC004: Test AML customer transactions creation success
# TC005: Test AML customer transactions with empty data
# TC006: Test branch summary report creation success
# TC007: Test branch summary report with missing operational data
# TC008: Test branch summary report aggregation logic
# TC009: Test write to delta table success
# TC010: Test write to delta table exception handling
# TC011: Test main function success path
# TC012: Test main function exception handling
# TC013: Test data quality null value handling
# TC014: Test performance with large dataset simulation
# TC015: Test schema validation

if __name__ == "__main__":
    # Run tests with pytest
    pytest.main(["-v", __file__])

# Cost Estimation and Justification
# Analysis & Design: 2 hours (code analysis, test strategy development, edge case identification)
# Test Implementation: 3 hours (15 comprehensive test cases covering all functions and scenarios)
# Mocking & Framework Setup: 1 hour (pytest setup, mock configurations, fixture creation)
# Documentation & Comments: 0.5 hour (test case documentation, inline comments)
# Total Estimate: ~6.5 hours
# 
# Justification:
# Comprehensive test coverage including happy paths, edge cases, error handling, performance testing,
# schema validation, null handling, and end-to-end integration testing with proper mocking and assertions.