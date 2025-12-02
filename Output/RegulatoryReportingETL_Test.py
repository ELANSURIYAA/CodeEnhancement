#====================================================================
# Author: Ascendion AAVA
# Date: 
# Description: Python-based test script for RegulatoryReportingETL_Pipeline.py (insert and update scenarios)
#====================================================================

import sys
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from RegulatoryReportingETL_Pipeline import create_branch_summary_report, get_sample_data

# Helper function for pretty printing tables

def print_markdown_table(headers, rows):
    print('| ' + ' | '.join(headers) + ' |')
    print('| ' + ' | '.join(['-' * len(h) for h in headers]) + ' |')
    for row in rows:
        print('| ' + ' | '.join(str(x) for x in row) + ' |')

# Scenario 1: Insert test

def test_insert(spark):
    # All rows are new (simulate new branches)
    transaction_data = [
        (1001, 101, "DEPOSIT", 500.0, "2023-03-01", "Initial Deposit"),
        (1002, 102, "WITHDRAWAL", 300.0, "2023-03-02", "ATM Withdrawal"),
    ]
    transaction_schema = StructType([
        StructField("TRANSACTION_ID", IntegerType()),
        StructField("ACCOUNT_ID", IntegerType()),
        StructField("TRANSACTION_TYPE", StringType()),
        StructField("AMOUNT", DoubleType()),
        StructField("TRANSACTION_DATE", StringType()),
        StructField("DESCRIPTION", StringType()),
    ])
    transaction_df = spark.createDataFrame(transaction_data, schema=transaction_schema)

    # Sample branch operational details (all active)
    bod_data = [
        (10, "North", "Alice", "2023-05-01", "Y"),
        (20, "South", "Bob", "2023-06-01", "Y"),
    ]
    bod_schema = StructType([
        StructField("BRANCH_ID", IntegerType()),
        StructField("REGION", StringType()),
        StructField("MANAGER_NAME", StringType()),
        StructField("LAST_AUDIT_DATE", StringType()),
        StructField("IS_ACTIVE", StringType()),
    ])
    bod_df = spark.createDataFrame(bod_data, schema=bod_schema)

    # Other sources
    customer_df, account_df, _, branch_df, _ = get_sample_data(spark)

    # Run transformation
    branch_summary_df = create_branch_summary_report(transaction_df, account_df, branch_df, bod_df)
    result = branch_summary_df.collect()
    return branch_summary_df, result

# Scenario 2: Update test

def test_update(spark):
    # Simulate update: Branch 10 already exists, update REGION and LAST_AUDIT_DATE
    transaction_data = [
        (1003, 101, "DEPOSIT", 700.0, "2023-04-01", "Second Deposit"),
        (1004, 102, "WITHDRAWAL", 100.0, "2023-04-02", "ATM Withdrawal"),
    ]
    transaction_schema = StructType([
        StructField("TRANSACTION_ID", IntegerType()),
        StructField("ACCOUNT_ID", IntegerType()),
        StructField("TRANSACTION_TYPE", StringType()),
        StructField("AMOUNT", DoubleType()),
        StructField("TRANSACTION_DATE", StringType()),
        StructField("DESCRIPTION", StringType()),
    ])
    transaction_df = spark.createDataFrame(transaction_data, schema=transaction_schema)

    bod_data = [
        (10, "East", "Carol", "2023-07-01", "Y"),  # Updated REGION and LAST_AUDIT_DATE for branch 10
        (20, "South", "Bob", "2023-06-01", "N"),
    ]
    bod_schema = StructType([
        StructField("BRANCH_ID", IntegerType()),
        StructField("REGION", StringType()),
        StructField("MANAGER_NAME", StringType()),
        StructField("LAST_AUDIT_DATE", StringType()),
        StructField("IS_ACTIVE", StringType()),
    ])
    bod_df = spark.createDataFrame(bod_data, schema=bod_schema)

    customer_df, account_df, _, branch_df, _ = get_sample_data(spark)

    branch_summary_df = create_branch_summary_report(transaction_df, account_df, branch_df, bod_df)
    result = branch_summary_df.collect()
    return branch_summary_df, result

# Run tests and print markdown report

def main():
    spark = SparkSession.builder.appName("TestBranchSummaryReport").getOrCreate()

    print("## Test Report\n")

    # Scenario 1: Insert
    print("### Scenario 1: Insert")
    branch_summary_df, result = test_insert(spark)
    input_rows = [
        (1001, 101, "DEPOSIT", 500.0, "2023-03-01", "Initial Deposit"),
        (1002, 102, "WITHDRAWAL", 300.0, "2023-03-02", "ATM Withdrawal"),
    ]
    print("Input:")
    print_markdown_table([
        "TRANSACTION_ID", "ACCOUNT_ID", "TRANSACTION_TYPE", "AMOUNT", "TRANSACTION_DATE", "DESCRIPTION"
    ], input_rows)
    print("Output:")
    out_rows = [tuple(row[x] for x in ["BRANCH_ID", "BRANCH_NAME", "TOTAL_TRANSACTIONS", "TOTAL_AMOUNT", "REGION", "LAST_AUDIT_DATE"]) for row in result]
    print_markdown_table([
        "BRANCH_ID", "BRANCH_NAME", "TOTAL_TRANSACTIONS", "TOTAL_AMOUNT", "REGION", "LAST_AUDIT_DATE"
    ], out_rows)
    # Simple validation: REGION and LAST_AUDIT_DATE populated for IS_ACTIVE = 'Y'
    status = "PASS" if all(row[4] is not None and row[5] is not None for row in out_rows) else "FAIL"
    print(f"Status: {status}\n")

    # Scenario 2: Update
    print("### Scenario 2: Update")
    branch_summary_df, result = test_update(spark)
    input_rows = [
        (1003, 101, "DEPOSIT", 700.0, "2023-04-01", "Second Deposit"),
        (1004, 102, "WITHDRAWAL", 100.0, "2023-04-02", "ATM Withdrawal"),
    ]
    print("Input:")
    print_markdown_table([
        "TRANSACTION_ID", "ACCOUNT_ID", "TRANSACTION_TYPE", "AMOUNT", "TRANSACTION_DATE", "DESCRIPTION"
    ], input_rows)
    print("Output:")
    out_rows = [tuple(row[x] for x in ["BRANCH_ID", "BRANCH_NAME", "TOTAL_TRANSACTIONS", "TOTAL_AMOUNT", "REGION", "LAST_AUDIT_DATE"]) for row in result]
    print_markdown_table([
        "BRANCH_ID", "BRANCH_NAME", "TOTAL_TRANSACTIONS", "TOTAL_AMOUNT", "REGION", "LAST_AUDIT_DATE"
    ], out_rows)
    # Validate: REGION and LAST_AUDIT_DATE for branch 10 updated to 'East' and '2023-07-01'
    status = "PASS" if any(row[0] == 10 and row[4] == "East" and row[5] == "2023-07-01" for row in out_rows) else "FAIL"
    print(f"Status: {status}\n")

    spark.stop()

if __name__ == "__main__":
    main()
