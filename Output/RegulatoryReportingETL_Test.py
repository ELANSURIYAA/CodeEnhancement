====================================================================
Author: Ascendion AAVA
Date: 
Description: Python test script for RegulatoryReportingETL_Pipeline.py. Validates insert and update scenarios for BRANCH_SUMMARY_REPORT logic.
====================================================================

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum, when

# Import the ETL function from pipeline (simulated here)
def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, branch_df: DataFrame, branch_operational_details_df: DataFrame) -> DataFrame:
    branch_summary = transaction_df.join(account_df, "ACCOUNT_ID") \
        .join(branch_df, "BRANCH_ID") \
        .groupBy("BRANCH_ID", "BRANCH_NAME") \
        .agg(count("*").alias("TOTAL_TRANSACTIONS"), sum("AMOUNT").alias("TOTAL_AMOUNT"))
    branch_summary = branch_summary.join(branch_operational_details_df, "BRANCH_ID", "left")
    branch_summary = branch_summary.withColumn(
        "REGION", when(col("IS_ACTIVE") == "Y", col("REGION")).otherwise(None)
    ).withColumn(
        "LAST_AUDIT_DATE", when(col("IS_ACTIVE") == "Y", col("LAST_AUDIT_DATE")).otherwise(None)
    )
    return branch_summary.select(
        "BRANCH_ID", "BRANCH_NAME", "TOTAL_TRANSACTIONS", "TOTAL_AMOUNT", "REGION", "LAST_AUDIT_DATE"
    )

def run_test_scenario(spark, scenario_name, transaction_data, account_data, branch_data, operational_data, expected_output):
    transaction_df = spark.createDataFrame(transaction_data, ["TRANSACTION_ID", "ACCOUNT_ID", "TRANSACTION_TYPE", "AMOUNT", "TRANSACTION_DATE", "DESCRIPTION"])
    account_df = spark.createDataFrame(account_data, ["ACCOUNT_ID", "CUSTOMER_ID", "BRANCH_ID", "ACCOUNT_NUMBER", "ACCOUNT_TYPE", "BALANCE", "OPENED_DATE"])
    branch_df = spark.createDataFrame(branch_data, ["BRANCH_ID", "BRANCH_NAME", "BRANCH_CODE", "CITY", "STATE", "COUNTRY"])
    operational_df = spark.createDataFrame(operational_data, ["BRANCH_ID", "REGION", "MANAGER_NAME", "LAST_AUDIT_DATE", "IS_ACTIVE"])
    result_df = create_branch_summary_report(transaction_df, account_df, branch_df, operational_df)
    result = [tuple(row) for row in result_df.collect()]
    passed = sorted(result) == sorted(expected_output)
    return result, passed

def main():
    spark = SparkSession.builder.master("local[1]").appName("RegulatoryReportingETL_Test").getOrCreate()
    # Scenario 1: Insert (new branch summary)
    transaction_data_1 = [
        (1001, 101, "DEPOSIT", 500.00, "2023-02-01", "Salary"),
        (1002, 102, "WITHDRAWAL", 300.00, "2023-02-02", "Bill Payment")
    ]
    account_data_1 = [
        (101, 1, 10, "ACCT001", "SAVINGS", 1000.00, "2023-01-01"),
        (102, 2, 20, "ACCT002", "CURRENT", 2000.00, "2023-01-02")
    ]
    branch_data_1 = [
        (10, "Central", "CEN001", "NYC", "NY", "USA"),
        (20, "West", "WST001", "LA", "CA", "USA")
    ]
    operational_data_1 = [
        (10, "East", "John Doe", "2023-03-01", "Y"),
        (20, "West", "Jane Smith", "2023-03-05", "N")
    ]
    expected_output_1 = [
        (10, "Central", 1, 500.00, "East", "2023-03-01"),
        (20, "West", 1, 300.00, None, None)
    ]
    result_1, passed_1 = run_test_scenario(
        spark, "Insert", transaction_data_1, account_data_1, branch_data_1, operational_data_1, expected_output_1
    )
    # Scenario 2: Update (existing branch summary updated)
    transaction_data_2 = [
        (1003, 101, "DEPOSIT", 200.00, "2023-03-10", "Bonus"), # update for branch 10
        (1004, 102, "DEPOSIT", 150.00, "2023-03-11", "Refund") # update for branch 20
    ]
    account_data_2 = account_data_1
    branch_data_2 = branch_data_1
    operational_data_2 = [
        (10, "East", "John Doe", "2023-03-01", "Y"),
        (20, "West", "Jane Smith", "2023-03-05", "Y")
    ]
    expected_output_2 = [
        (10, "Central", 1, 200.00, "East", "2023-03-01"),
        (20, "West", 1, 150.00, "West", "2023-03-05")
    ]
    result_2, passed_2 = run_test_scenario(
        spark, "Update", transaction_data_2, account_data_2, branch_data_2, operational_data_2, expected_output_2
    )
    # Markdown report
    print("""
## Test Report
### Scenario 1: Insert
Input:
| BRANCH_ID | BRANCH_NAME | TRANSACTION_ID | AMOUNT | REGION | IS_ACTIVE |
|-----------|-------------|---------------|--------|--------|----------|
| 10        | Central     | 1001          | 500.00 | East   | Y        |
| 20        | West        | 1002          | 300.00 | West   | N        |
Output:
| BRANCH_ID | BRANCH_NAME | TOTAL_TRANSACTIONS | TOTAL_AMOUNT | REGION | LAST_AUDIT_DATE |
|-----------|-------------|--------------------|--------------|--------|-----------------|
""")
    for row in result_1:
        print(f"| {row[0]}        | {row[1]}     | {row[2]}                  | {row[3]}        | {row[4]}   | {row[5]}         |")
    print(f"Status: {'PASS' if passed_1 else 'FAIL'}\n")
    print("""
### Scenario 2: Update
Input:
| BRANCH_ID | BRANCH_NAME | TRANSACTION_ID | AMOUNT | REGION | IS_ACTIVE |
|-----------|-------------|---------------|--------|--------|----------|
| 10        | Central     | 1003          | 200.00 | East   | Y        |
| 20        | West        | 1004          | 150.00 | West   | Y        |
Output:
| BRANCH_ID | BRANCH_NAME | TOTAL_TRANSACTIONS | TOTAL_AMOUNT | REGION | LAST_AUDIT_DATE |
|-----------|-------------|--------------------|--------------|--------|-----------------|
""")
    for row in result_2:
        print(f"| {row[0]}        | {row[1]}     | {row[2]}                  | {row[3]}        | {row[4]}   | {row[5]}         |")
    print(f"Status: {'PASS' if passed_2 else 'FAIL'}\n")
    spark.stop()

if __name__ == "__main__":
    main()
