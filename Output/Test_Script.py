====================================================================
Author: Ascendion AAVA
Date: <Leave it blank>
Description: Python test script for Enhanced PySpark ETL pipeline (without PyTest)
====================================================================

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, when, lit
from pyspark.sql.types import *
from datetime import date
import tempfile
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TestRunner:
    def __init__(self):
        self.spark = None
        self.results = []
        
    def setup(self):
        try:
            self.spark = SparkSession.getActiveSession()
            if not self.spark:
                self.spark = SparkSession.builder.appName("TestRunner").getOrCreate()
            logger.info("Spark session ready")
        except Exception as e:
            logger.error(f"Setup failed: {e}")
            raise
    
    def create_branch_summary(self, transaction_df, account_df, branch_df, branch_operational_df):
        """Enhanced branch summary with operational details"""
        base = transaction_df.join(account_df, "ACCOUNT_ID") \
                            .join(branch_df, "BRANCH_ID") \
                            .groupBy("BRANCH_ID", "BRANCH_NAME") \
                            .agg(count("*").alias("TOTAL_TRANSACTIONS"),
                                 spark_sum("AMOUNT").alias("TOTAL_AMOUNT"))
        
        enhanced = base.join(branch_operational_df, 
                           base["BRANCH_ID"] == branch_operational_df["BRANCH_ID"], "left") \
                      .select(base["BRANCH_ID"], base["BRANCH_NAME"],
                             base["TOTAL_TRANSACTIONS"], base["TOTAL_AMOUNT"],
                             when(branch_operational_df["IS_ACTIVE"] == "Y", 
                                  branch_operational_df["REGION"]).otherwise(lit(None)).alias("REGION"),
                             when(branch_operational_df["IS_ACTIVE"] == "Y", 
                                  branch_operational_df["LAST_AUDIT_DATE"]).otherwise(lit(None)).alias("LAST_AUDIT_DATE"))
        return enhanced
    
    def test_scenario_1_insert(self):
        """Test Scenario 1: Insert new records"""
        logger.info("Testing Scenario 1: Insert")
        
        # Test data for new branches
        branch_data = [(201, "Test Branch 1", "TB001", "Boston", "MA", "USA"),
                       (202, "Test Branch 2", "TB002", "Seattle", "WA", "USA")]
        account_data = [(2001, 1, 201, "ACC2001", "CHECKING", 3000.0, date(2023, 5, 1)),
                        (2002, 2, 202, "ACC2002", "SAVINGS", 8000.0, date(2023, 5, 2))]
        transaction_data = [(20001, 2001, "DEPOSIT", 1500.0, date(2023, 5, 10), "New deposit"),
                           (20002, 2002, "TRANSFER", 2500.0, date(2023, 5, 11), "New transfer")]
        operational_data = [(201, "Southeast", "David Mgr", date(2023, 4, 10), "Y"),
                           (202, "Northwest", "Eva Mgr", date(2023, 4, 15), "Y")]
        
        # Create schemas
        branch_schema = StructType([StructField("BRANCH_ID", IntegerType()),
                                   StructField("BRANCH_NAME", StringType()),
                                   StructField("BRANCH_CODE", StringType()),
                                   StructField("CITY", StringType()),
                                   StructField("STATE", StringType()),
                                   StructField("COUNTRY", StringType())])
        
        account_schema = StructType([StructField("ACCOUNT_ID", IntegerType()),
                                    StructField("CUSTOMER_ID", IntegerType()),
                                    StructField("BRANCH_ID", IntegerType()),
                                    StructField("ACCOUNT_NUMBER", StringType()),
                                    StructField("ACCOUNT_TYPE", StringType()),
                                    StructField("BALANCE", DoubleType()),
                                    StructField("OPENED_DATE", DateType())])
        
        transaction_schema = StructType([StructField("TRANSACTION_ID", IntegerType()),
                                        StructField("ACCOUNT_ID", IntegerType()),
                                        StructField("TRANSACTION_TYPE", StringType()),
                                        StructField("AMOUNT", DoubleType()),
                                        StructField("TRANSACTION_DATE", DateType()),
                                        StructField("DESCRIPTION", StringType())])
        
        operational_schema = StructType([StructField("BRANCH_ID", IntegerType()),
                                        StructField("REGION", StringType()),
                                        StructField("MANAGER_NAME", StringType()),
                                        StructField("LAST_AUDIT_DATE", DateType()),
                                        StructField("IS_ACTIVE", StringType())])
        
        # Create DataFrames
        branch_df = self.spark.createDataFrame(branch_data, branch_schema)
        account_df = self.spark.createDataFrame(account_data, account_schema)
        transaction_df = self.spark.createDataFrame(transaction_data, transaction_schema)
        operational_df = self.spark.createDataFrame(operational_data, operational_schema)
        
        # Create summary
        result_df = self.create_branch_summary(transaction_df, account_df, branch_df, operational_df)
        results = result_df.collect()
        
        # Validate
        expected_branches = [201, 202]
        actual_branches = [r['BRANCH_ID'] for r in results]
        regions_ok = all(r['REGION'] is not None for r in results)
        
        status = "PASS" if all(b in actual_branches for b in expected_branches) and regions_ok else "FAIL"
        
        self.results.append({
            'scenario': 'Scenario 1: Insert',
            'input_branches': expected_branches,
            'output_branches': actual_branches,
            'status': status,
            'details': [(r['BRANCH_ID'], r['BRANCH_NAME'], r['REGION']) for r in results]
        })
        
        logger.info(f"Scenario 1 status: {status}")
    
    def test_scenario_2_update(self):
        """Test Scenario 2: Update existing records"""
        logger.info("Testing Scenario 2: Update")
        
        # Updated data for existing branches
        branch_data = [(101, "Downtown Updated", "DT001", "New York", "NY", "USA")]
        account_data = [(3001, 1, 101, "ACC3001", "CHECKING", 4000.0, date(2023, 5, 1))]
        transaction_data = [(30001, 3001, "DEPOSIT", 2000.0, date(2023, 5, 15), "Updated deposit")]
        operational_data = [(101, "Northeast Updated", "Alice Updated", date(2023, 4, 20), "Y")]
        
        # Create schemas (same as scenario 1)
        branch_schema = StructType([StructField("BRANCH_ID", IntegerType()),
                                   StructField("BRANCH_NAME", StringType()),
                                   StructField("BRANCH_CODE", StringType()),
                                   StructField("CITY", StringType()),
                                   StructField("STATE", StringType()),
                                   StructField("COUNTRY", StringType())])
        
        account_schema = StructType([StructField("ACCOUNT_ID", IntegerType()),
                                    StructField("CUSTOMER_ID", IntegerType()),
                                    StructField("BRANCH_ID", IntegerType()),
                                    StructField("ACCOUNT_NUMBER", StringType()),
                                    StructField("ACCOUNT_TYPE", StringType()),
                                    StructField("BALANCE", DoubleType()),
                                    StructField("OPENED_DATE", DateType())])
        
        transaction_schema = StructType([StructField("TRANSACTION_ID", IntegerType()),
                                        StructField("ACCOUNT_ID", IntegerType()),
                                        StructField("TRANSACTION_TYPE", StringType()),
                                        StructField("AMOUNT", DoubleType()),
                                        StructField("TRANSACTION_DATE", DateType()),
                                        StructField("DESCRIPTION", StringType())])
        
        operational_schema = StructType([StructField("BRANCH_ID", IntegerType()),
                                        StructField("REGION", StringType()),
                                        StructField("MANAGER_NAME", StringType()),
                                        StructField("LAST_AUDIT_DATE", DateType()),
                                        StructField("IS_ACTIVE", StringType())])
        
        # Create DataFrames
        branch_df = self.spark.createDataFrame(branch_data, branch_schema)
        account_df = self.spark.createDataFrame(account_data, account_schema)
        transaction_df = self.spark.createDataFrame(transaction_data, transaction_schema)
        operational_df = self.spark.createDataFrame(operational_data, operational_schema)
        
        # Create summary
        result_df = self.create_branch_summary(transaction_df, account_df, branch_df, operational_df)
        results = result_df.collect()
        
        # Validate update
        updated_branch = results[0] if results else None
        update_success = (updated_branch and 
                         updated_branch['BRANCH_ID'] == 101 and 
                         "Updated" in updated_branch['BRANCH_NAME'] and
                         "Updated" in updated_branch['REGION'])
        
        status = "PASS" if update_success else "FAIL"
        
        self.results.append({
            'scenario': 'Scenario 2: Update',
            'expected_branch': 101,
            'updated_name': updated_branch['BRANCH_NAME'] if updated_branch else None,
            'updated_region': updated_branch['REGION'] if updated_branch else None,
            'status': status
        })
        
        logger.info(f"Scenario 2 status: {status}")
    
    def generate_report(self):
        """Generate markdown test report"""
        report = "# Test Report\n\n"
        
        for result in self.results:
            report += f"## {result['scenario']}\n\n"
            
            if result['scenario'] == 'Scenario 1: Insert':
                report += "Input:\n"
                report += "| BRANCH_ID | BRANCH_NAME |\n"
                report += "|-----------|-------------|\n"
                for branch_id in result['input_branches']:
                    name = f"Test Branch {branch_id - 200}"
                    report += f"| {branch_id} | {name} |\n"
                
                report += "\nOutput:\n"
                report += "| BRANCH_ID | BRANCH_NAME | REGION |\n"
                report += "|-----------|-------------|--------|\n"
                for detail in result['details']:
                    report += f"| {detail[0]} | {detail[1]} | {detail[2]} |\n"
                    
            elif result['scenario'] == 'Scenario 2: Update':
                report += "Input:\n"
                report += "| BRANCH_ID | BRANCH_NAME |\n"
                report += "|-----------|-------------|\n"
                report += f"| {result['expected_branch']} | Downtown Updated |\n"
                
                report += "\nOutput:\n"
                report += "| BRANCH_ID | BRANCH_NAME | REGION |\n"
                report += "|-----------|-------------|--------|\n"
                report += f"| {result['expected_branch']} | {result['updated_name']} | {result['updated_region']} |\n"
            
            report += f"\nStatus: **{result['status']}**\n\n"
        
        return report
    
    def run_all_tests(self):
        """Run all test scenarios"""
        logger.info("Starting test execution")
        
        try:
            self.setup()
            self.test_scenario_1_insert()
            self.test_scenario_2_update()
            
            report = self.generate_report()
            print("\n" + "="*50)
            print("TEST EXECUTION COMPLETED")
            print("="*50)
            print(report)
            
            return report
            
        except Exception as e:
            logger.error(f"Test execution failed: {e}")
            return f"# Test Report\n\nTest execution failed: {e}\n"
        
        finally:
            if self.spark:
                self.spark.stop()

if __name__ == "__main__":
    runner = TestRunner()
    runner.run_all_tests()