====================================================================
Author: Ascendion AAVA
Date: <Leave it blank>
Description: Python test script for validating RegulatoryReportingETL Pipeline functionality
====================================================================

import sys
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum, when, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, DateType, LongType, DoubleType
from datetime import date, datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ETLTester:
    """
    Test class for validating ETL pipeline functionality
    """
    
    def __init__(self):
        self.spark = self._get_spark_session()
        self.test_results = []
    
    def _get_spark_session(self) -> SparkSession:
        """
        Initialize Spark session for testing
        """
        try:
            spark = SparkSession.getActiveSession()
            if spark is None:
                spark = SparkSession.builder \
                    .appName("ETL_Testing") \
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                    .getOrCreate()
            
            spark.conf.set("spark.sql.adaptive.enabled", "true")
            return spark
        except Exception as e:
            logger.error(f"Error creating Spark session: {e}")
            raise
    
    def create_test_data_scenario_1(self) -> tuple:
        """
        Create test data for Scenario 1: Insert new records
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
            (201, "Test Branch A", "TB001", "Boston", "MA", "USA"),
            (202, "Test Branch B", "TB002", "Seattle", "WA", "USA")
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
            (2001, 1, 201, "ACC002001", "SAVINGS", 10000.00, date(2023, 5, 1)),
            (2002, 2, 202, "ACC002002", "CHECKING", 5000.00, date(2023, 5, 2))
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
            (20001, 2001, "DEPOSIT", 2000.00, date(2023, 5, 10), "New deposit"),
            (20002, 2001, "WITHDRAWAL", 500.00, date(2023, 5, 11), "ATM withdrawal"),
            (20003, 2002, "DEPOSIT", 1000.00, date(2023, 5, 10), "Check deposit")
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
            (201, "Northeast", "Test Manager A", date(2023, 4, 15), "Y"),
            (202, "Northwest", "Test Manager B", date(2023, 4, 20), "Y")
        ]
        
        branch_operational_df = self.spark.createDataFrame(branch_operational_data, branch_operational_schema)
        
        return transaction_df, account_df, branch_df, branch_operational_df
    
    def create_test_data_scenario_2(self) -> tuple:
        """
        Create test data for Scenario 2: Update existing records
        """
        logger.info("Creating test data for Scenario 2: Update")
        
        # Use same branch IDs but with updated operational details
        branch_schema = StructType([
            StructField("BRANCH_ID", IntegerType(), True),
            StructField("BRANCH_NAME", StringType(), True),
            StructField("BRANCH_CODE", StringType(), True),
            StructField("CITY", StringType(), True),
            StructField("STATE", StringType(), True),
            StructField("COUNTRY", StringType(), True)
        ])
        
        branch_data = [
            (201, "Updated Branch A", "TB001", "Boston", "MA", "USA"),  # Updated name
            (202, "Updated Branch B", "TB002", "Seattle", "WA", "USA")   # Updated name
        ]
        
        branch_df = self.spark.createDataFrame(branch_data, branch_schema)
        
        # Account data (same structure)
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
            (2001, 1, 201, "ACC002001", "SAVINGS", 12000.00, date(2023, 5, 1)),  # Updated balance
            (2002, 2, 202, "ACC002002", "CHECKING", 6000.00, date(2023, 5, 2))   # Updated balance
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
            (20004, 2001, "DEPOSIT", 3000.00, date(2023, 5, 15), "Updated deposit"),
            (20005, 2002, "TRANSFER", 2000.00, date(2023, 5, 16), "Updated transfer")
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
            (201, "Northeast Updated", "Updated Manager A", date(2023, 5, 15), "Y"),  # Updated region and manager
            (202, "Northwest Updated", "Updated Manager B", date(2023, 5, 20), "N")   # Updated and set to inactive
        ]
        
        branch_operational_df = self.spark.createDataFrame(branch_operational_data, branch_operational_schema)
        
        return transaction_df, account_df, branch_df, branch_operational_df
    
    def create_branch_summary_report(self, transaction_df: DataFrame, account_df: DataFrame, 
                                   branch_df: DataFrame, branch_operational_df: DataFrame) -> DataFrame:
        """
        Create branch summary report (same logic as in main pipeline)
        """
        # Base summary
        base_summary = transaction_df.join(account_df, "ACCOUNT_ID", "inner") \
                                     .join(branch_df, "BRANCH_ID", "inner") \
                                     .groupBy("BRANCH_ID", "BRANCH_NAME") \
                                     .agg(
                                         count("*").alias("TOTAL_TRANSACTIONS"),
                                         sum("AMOUNT").alias("TOTAL_AMOUNT")
                                     )
        
        # Enhanced summary with operational details
        enhanced_summary = base_summary.join(
            branch_operational_df, 
            "BRANCH_ID", 
            "left"
        ).select(
            col("BRANCH_ID").cast(LongType()),
            col("BRANCH_NAME"),
            col("TOTAL_TRANSACTIONS"),
            col("TOTAL_AMOUNT").cast(DoubleType()),
            when(col("IS_ACTIVE") == "Y", col("REGION")).otherwise(lit(None)).alias("REGION"),
            when(col("IS_ACTIVE") == "Y", col("LAST_AUDIT_DATE").cast(StringType())).otherwise(lit(None)).alias("LAST_AUDIT_DATE")
        )
        
        return enhanced_summary
    
    def test_scenario_1_insert(self):
        """
        Test Scenario 1: Insert new records
        """
        logger.info("Testing Scenario 1: Insert")
        
        try:
            # Create test data
            transaction_df, account_df, branch_df, branch_operational_df = self.create_test_data_scenario_1()
            
            # Execute ETL logic
            result_df = self.create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_df)
            
            # Collect results for validation
            results = result_df.collect()
            
            # Validate results
            expected_branches = {201, 202}
            actual_branches = {row['BRANCH_ID'] for row in results}
            
            # Check if all expected branches are present
            if expected_branches == actual_branches:
                # Check specific values
                branch_201 = next((row for row in results if row['BRANCH_ID'] == 201), None)
                branch_202 = next((row for row in results if row['BRANCH_ID'] == 202), None)
                
                validation_passed = (
                    branch_201 is not None and
                    branch_202 is not None and
                    branch_201['REGION'] == 'Northeast' and
                    branch_202['REGION'] == 'Northwest' and
                    branch_201['TOTAL_TRANSACTIONS'] == 2 and
                    branch_202['TOTAL_TRANSACTIONS'] == 1
                )
                
                status = "PASS" if validation_passed else "FAIL"
            else:
                status = "FAIL"
            
            # Store test result
            self.test_results.append({
                'scenario': 'Scenario 1: Insert',
                'input_data': {
                    'branches': [{'id': 201, 'name': 'Test Branch A'}, {'id': 202, 'name': 'Test Branch B'}],
                    'transactions': [{'id': 20001, 'amount': 2000.00}, {'id': 20002, 'amount': 500.00}, {'id': 20003, 'amount': 1000.00}]
                },
                'output_data': results,
                'status': status
            })
            
            logger.info(f"Scenario 1 completed with status: {status}")
            
        except Exception as e:
            logger.error(f"Scenario 1 failed with error: {e}")
            self.test_results.append({
                'scenario': 'Scenario 1: Insert',
                'input_data': 'Error in test execution',
                'output_data': str(e),
                'status': 'FAIL'
            })
    
    def test_scenario_2_update(self):
        """
        Test Scenario 2: Update existing records
        """
        logger.info("Testing Scenario 2: Update")
        
        try:
            # Create test data
            transaction_df, account_df, branch_df, branch_operational_df = self.create_test_data_scenario_2()
            
            # Execute ETL logic
            result_df = self.create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_df)
            
            # Collect results for validation
            results = result_df.collect()
            
            # Validate results
            expected_branches = {201, 202}
            actual_branches = {row['BRANCH_ID'] for row in results}
            
            # Check if all expected branches are present
            if expected_branches == actual_branches:
                # Check specific values for updates
                branch_201 = next((row for row in results if row['BRANCH_ID'] == 201), None)
                branch_202 = next((row for row in results if row['BRANCH_ID'] == 202), None)
                
                validation_passed = (
                    branch_201 is not None and
                    branch_202 is not None and
                    branch_201['BRANCH_NAME'] == 'Updated Branch A' and
                    branch_202['BRANCH_NAME'] == 'Updated Branch B' and
                    branch_201['REGION'] == 'Northeast Updated' and  # Active branch should have region
                    branch_202['REGION'] is None and  # Inactive branch should have null region
                    branch_201['TOTAL_TRANSACTIONS'] == 1 and
                    branch_202['TOTAL_TRANSACTIONS'] == 1
                )
                
                status = "PASS" if validation_passed else "FAIL"
            else:
                status = "FAIL"
            
            # Store test result
            self.test_results.append({
                'scenario': 'Scenario 2: Update',
                'input_data': {
                    'branches': [{'id': 201, 'name': 'Updated Branch A'}, {'id': 202, 'name': 'Updated Branch B'}],
                    'transactions': [{'id': 20004, 'amount': 3000.00}, {'id': 20005, 'amount': 2000.00}]
                },
                'output_data': results,
                'status': status
            })
            
            logger.info(f"Scenario 2 completed with status: {status}")
            
        except Exception as e:
            logger.error(f"Scenario 2 failed with error: {e}")
            self.test_results.append({
                'scenario': 'Scenario 2: Update',
                'input_data': 'Error in test execution',
                'output_data': str(e),
                'status': 'FAIL'
            })
    
    def generate_markdown_report(self) -> str:
        """
        Generate markdown test report
        """
        report = "# Test Report\n\n"
        
        for result in self.test_results:
            report += f"## {result['scenario']}\n\n"
            
            # Input section
            report += "### Input:\n"
            if isinstance(result['input_data'], dict):
                if 'branches' in result['input_data']:
                    report += "**Branches:**\n"
                    report += "| Branch ID | Branch Name |\n"
                    report += "|-----------|-------------|\n"
                    for branch in result['input_data']['branches']:
                        report += f"| {branch['id']} | {branch['name']} |\n"
                    report += "\n"
                
                if 'transactions' in result['input_data']:
                    report += "**Transactions:**\n"
                    report += "| Transaction ID | Amount |\n"
                    report += "|----------------|--------|\n"
                    for txn in result['input_data']['transactions']:
                        report += f"| {txn['id']} | {txn['amount']} |\n"
                    report += "\n"
            else:
                report += f"{result['input_data']}\n\n"
            
            # Output section
            report += "### Output:\n"
            if isinstance(result['output_data'], list) and len(result['output_data']) > 0:
                report += "| Branch ID | Branch Name | Total Transactions | Total Amount | Region | Last Audit Date |\n"
                report += "|-----------|-------------|-------------------|--------------|--------|----------------|\n"
                for row in result['output_data']:
                    region = row['REGION'] if row['REGION'] is not None else 'NULL'
                    audit_date = row['LAST_AUDIT_DATE'] if row['LAST_AUDIT_DATE'] is not None else 'NULL'
                    report += f"| {row['BRANCH_ID']} | {row['BRANCH_NAME']} | {row['TOTAL_TRANSACTIONS']} | {row['TOTAL_AMOUNT']} | {region} | {audit_date} |\n"
            else:
                report += f"{result['output_data']}\n"
            
            report += "\n"
            
            # Status section
            report += f"### Status: **{result['status']}**\n\n"
            report += "---\n\n"
        
        return report
    
    def run_all_tests(self):
        """
        Run all test scenarios
        """
        logger.info("Starting ETL test execution")
        
        # Run test scenarios
        self.test_scenario_1_insert()
        self.test_scenario_2_update()
        
        # Generate and print report
        report = self.generate_markdown_report()
        print("\n" + "="*80)
        print("ETL TEST EXECUTION RESULTS")
        print("="*80)
        print(report)
        
        # Summary
        passed_tests = sum(1 for result in self.test_results if result['status'] == 'PASS')
        total_tests = len(self.test_results)
        
        print(f"\n## Test Summary")
        print(f"**Total Tests:** {total_tests}")
        print(f"**Passed:** {passed_tests}")
        print(f"**Failed:** {total_tests - passed_tests}")
        print(f"**Success Rate:** {(passed_tests/total_tests)*100:.1f}%")
        
        logger.info(f"Test execution completed. {passed_tests}/{total_tests} tests passed.")
        
        return report

def main():
    """
    Main test execution function
    """
    try:
        # Initialize tester
        tester = ETLTester()
        
        # Run all tests
        report = tester.run_all_tests()
        
        # Return report for external use
        return report
        
    except Exception as e:
        logger.error(f"Test execution failed: {e}")
        print(f"\nTest execution failed with error: {e}")
        return None

if __name__ == "__main__":
    main()
