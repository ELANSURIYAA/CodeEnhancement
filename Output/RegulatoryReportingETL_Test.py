====================================================================
Author: Ascendion AAVA
Date: 
Description: Python test script for RegulatoryReportingETL_Pipeline.py - validates insert and update logic for BRANCH_SUMMARY_REPORT
====================================================================

import sys
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from RegulatoryReportingETL_Pipeline import create_sample_data, create_branch_summary_report

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_insert_scenario(spark):
    """
    Scenario 1: Insert - All rows are new, validate output contains inserted rows.
    """
    customer_df, account_df, transaction_df, branch_df, branch_op_df = create_sample_data(spark)
    # All branch IDs are new for this test
    result_df = create_branch_summary_report(transaction_df, account_df, branch_df, branch_op_df)
    expected = [
        (10, "Central", 1, 500.0, "East", "2024-03-01"),
        (20, "West", 1, 300.0, None, None)
    ]
    result = [(row['BRANCH_ID'], row['BRANCH_NAME'], row['TOTAL_TRANSACTIONS'], row['TOTAL_AMOUNT'], row['REGION'], row['LAST_AUDIT_DATE']) for row in result_df.collect()]
    status = "PASS" if result == expected else "FAIL"
    return {
        "input": [
            {"BRANCH_ID": 10, "BRANCH_NAME": "Central", "IS_ACTIVE": "Y", "REGION": "East", "LAST_AUDIT_DATE": "2024-03-01"},
            {"BRANCH_ID": 20, "BRANCH_NAME": "West", "IS_ACTIVE": "N", "REGION": "West", "LAST_AUDIT_DATE": "2024-03-02"}
        ],
        "output": result,
        "status": status
    }

def test_update_scenario(spark):
    """
    Scenario 2: Update - Key already exists, validate correct update.
    """
    customer_df, account_df, transaction_df, branch_df, branch_op_df = create_sample_data(spark)
    # Simulate an update by adding a transaction for branch_id 10
    transaction_data_update = [
        (1003, 101, "DEPOSIT", 250.0, "2024-02-10", "Additional Deposit")
    ]
    transaction_update_df = spark.createDataFrame(transaction_data_update, ["TRANSACTION_ID", "ACCOUNT_ID", "TRANSACTION_TYPE", "AMOUNT", "TRANSACTION_DATE", "DESCRIPTION"])
    transaction_df = transaction_df.union(transaction_update_df)
    result_df = create_branch_summary_report(transaction_df, account_df, branch_df, branch_op_df)
    expected = [
        (10, "Central", 2, 750.0, "East", "2024-03-01"),
        (20, "West", 1, 300.0, None, None)
    ]
    result = [(row['BRANCH_ID'], row['BRANCH_NAME'], row['TOTAL_TRANSACTIONS'], row['TOTAL_AMOUNT'], row['REGION'], row['LAST_AUDIT_DATE']) for row in result_df.collect()]
    status = "PASS" if result == expected else "FAIL"
    return {
        "input": [
            {"BRANCH_ID": 10, "BRANCH_NAME": "Central", "IS_ACTIVE": "Y", "REGION": "East", "LAST_AUDIT_DATE": "2024-03-01"},
            {"BRANCH_ID": 20, "BRANCH_NAME": "West", "IS_ACTIVE": "N", "REGION": "West", "LAST_AUDIT_DATE": "2024-03-02"}
        ],
        "output": result,
        "status": status
    }

def markdown_report(scenario_name, test_result):
    md = f"## Test Report\n### {scenario_name}:\nInput:\n| BRANCH_ID | BRANCH_NAME | IS_ACTIVE | REGION | LAST_AUDIT_DATE |\n|-----------|-------------|-----------|--------|-----------------|\n"
    for row in test_result['input']:
        md += f"| {row['BRANCH_ID']} | {row['BRANCH_NAME']} | {row['IS_ACTIVE']} | {row['REGION']} | {row['LAST_AUDIT_DATE']} |\n"
    md += "Output:\n| BRANCH_ID | BRANCH_NAME | TOTAL_TRANSACTIONS | TOTAL_AMOUNT | REGION | LAST_AUDIT_DATE |\n|-----------|-------------|--------------------|-------------|--------|-----------------|\n"
    for row in test_result['output']:
        md += f"| {row[0]} | {row[1]} | {row[2]} | {row[3]} | {row[4]} | {row[5]} |\n"
    md += f"Status: {test_result['status']}\n"
    return md

def main():
    spark = SparkSession.builder.master("local[1]").appName("RegulatoryReportingETL_Test").getOrCreate()
    insert_result = test_insert_scenario(spark)
    update_result = test_update_scenario(spark)
    print(markdown_report("Scenario 1: Insert", insert_result))
    print(markdown_report("Scenario 2: Update", update_result))
    spark.stop()

if __name__ == "__main__":
    main()
