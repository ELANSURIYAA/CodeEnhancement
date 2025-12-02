====================================================================
Author: Ascendion AAVA
Date: 
Description: Python-based test script for RegulatoryReportingETL_Pipeline.py validating insert and update logic for BRANCH_SUMMARY_REPORT.
====================================================================

import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, when

# Import pipeline functions (assume same folder, or copy-paste for Databricks)
def get_spark_session(app_name: str = "RegulatoryReportingETL_Test"):
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark

def create_sample_branch_df(spark):
    data = [
        (101, "Main Branch", "MB001", "CityA", "StateA", "CountryA"),
        (102, "West Branch", "WB002", "CityB", "StateB", "CountryB")
    ]
    columns = ["BRANCH_ID", "BRANCH_NAME", "BRANCH_CODE", "CITY", "STATE", "COUNTRY"]
    return spark.createDataFrame(data, columns)

def create_sample_account_df(spark):
    data = [
        (1001, 1, 101, "ACC001", "Savings", 5000.0, "2024-05-01"),
        (1002, 2, 102, "ACC002", "Checking", 3000.0, "2024-05-02")
    ]
    columns = ["ACCOUNT_ID", "CUSTOMER_ID", "BRANCH_ID", "ACCOUNT_NUMBER", "ACCOUNT_TYPE", "BALANCE", "OPENED_DATE"]
    return spark.createDataFrame(data, columns)

def create_sample_transaction_df(spark):
    data = [
        (50001, 1001, "Deposit", 2000.0, "2024-05-10", "Salary"),
        (50002, 1002, "Withdrawal", 1500.0, "2024-05-11", "Bill Payment")
    ]
    columns = ["TRANSACTION_ID", "ACCOUNT_ID", "TRANSACTION_TYPE", "AMOUNT", "TRANSACTION_DATE", "DESCRIPTION"]
    return spark.createDataFrame(data, columns)

def create_sample_branch_operational_details_df_insert(spark):
    # Scenario 1: Insert (all new)
    data = [
        (101, "North", "John Doe", "2024-05-15", "Y"),
        (102, "West", "Jane Smith", "2024-05-16", "Y")
    ]
    columns = ["BRANCH_ID", "REGION", "MANAGER_NAME", "LAST_AUDIT_DATE", "IS_ACTIVE"]
    return spark.createDataFrame(data, columns)

def create_sample_branch_operational_details_df_update(spark):
    # Scenario 2: Update (key exists, IS_ACTIVE changes)
    data = [
        (101, "Central", "John Doe", "2024-05-20", "Y"),  # Updated region/audit date
        (102, "West", "Jane Smith", "2024-05-16", "N")     # IS_ACTIVE changed to N
    ]
    columns = ["BRANCH_ID", "REGION", "MANAGER_NAME", "LAST_AUDIT_DATE", "IS_ACTIVE"]
    return spark.createDataFrame(data, columns)

def create_branch_summary_report(
    transaction_df,
    account_df,
    branch_df,
    branch_operational_details_df
):
    base_df = transaction_df.join(account_df, "ACCOUNT_ID") \
                           .join(branch_df, "BRANCH_ID") \
                           .groupBy("BRANCH_ID", "BRANCH_NAME") \
                           .agg(
                               count("*").alias("TOTAL_TRANSACTIONS"),
                               sum("AMOUNT").alias("TOTAL_AMOUNT")
                           )
    merged_df = base_df.join(branch_operational_details_df, "BRANCH_ID", "left")
    merged_df = merged_df.withColumn(
        "REGION",
        when(col("IS_ACTIVE") == "Y", col("REGION")).otherwise(None)
    ).withColumn(
        "LAST_AUDIT_DATE",
        when(col("IS_ACTIVE") == "Y", col("LAST_AUDIT_DATE")).otherwise(None)
    )
    return merged_df.select(
        "BRANCH_ID", "BRANCH_NAME", "TOTAL_TRANSACTIONS", "TOTAL_AMOUNT", "REGION", "LAST_AUDIT_DATE"
    )

def run_insert_test(spark):
    branch_df = create_sample_branch_df(spark)
    account_df = create_sample_account_df(spark)
    transaction_df = create_sample_transaction_df(spark)
    branch_operational_details_df = create_sample_branch_operational_details_df_insert(spark)
    result_df = create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_details_df)
    result = result_df.orderBy("BRANCH_ID").collect()
    # Prepare input/output for markdown
    input_table = [
        [row.BRANCH_ID, row.REGION, row.LAST_AUDIT_DATE, row.IS_ACTIVE] for row in branch_operational_details_df.collect()
    ]
    output_table = [
        [row.BRANCH_ID, row.BRANCH_NAME, row.TOTAL_TRANSACTIONS, float(row.TOTAL_AMOUNT), row.REGION, row.LAST_AUDIT_DATE] for row in result
    ]
    # Validate: REGION and LAST_AUDIT_DATE should be populated for IS_ACTIVE='Y'
    status = "PASS"
    for row in output_table:
        if row[4] is None or row[5] is None:
            status = "FAIL"
    return input_table, output_table, status

def run_update_test(spark):
    branch_df = create_sample_branch_df(spark)
    account_df = create_sample_account_df(spark)
    transaction_df = create_sample_transaction_df(spark)
    branch_operational_details_df = create_sample_branch_operational_details_df_update(spark)
    result_df = create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_details_df)
    result = result_df.orderBy("BRANCH_ID").collect()
    input_table = [
        [row.BRANCH_ID, row.REGION, row.LAST_AUDIT_DATE, row.IS_ACTIVE] for row in branch_operational_details_df.collect()
    ]
    output_table = [
        [row.BRANCH_ID, row.BRANCH_NAME, row.TOTAL_TRANSACTIONS, float(row.TOTAL_AMOUNT), row.REGION, row.LAST_AUDIT_DATE] for row in result
    ]
    # Validate: REGION/LAST_AUDIT_DATE only populated for IS_ACTIVE='Y'
    status = "PASS"
    for row in output_table:
        # For BRANCH_ID 101, REGION should be 'Central', LAST_AUDIT_DATE '2024-05-20'; for 102, None
        if row[0] == 101 and (row[4] != 'Central' or row[5] != '2024-05-20'):
            status = "FAIL"
        if row[0] == 102 and (row[4] is not None or row[5] is not None):
            status = "FAIL"
    return input_table, output_table, status

if __name__ == "__main__":
    spark = get_spark_session()
    # Scenario 1: Insert
    input1, output1, status1 = run_insert_test(spark)
    # Scenario 2: Update
    input2, output2, status2 = run_update_test(spark)
    # Markdown report
    print("## Test Report\n")
    print("### Scenario 1: Insert\nInput:")
    print("| BRANCH_ID | REGION | LAST_AUDIT_DATE | IS_ACTIVE |")
    print("|-----------|--------|-----------------|-----------|")
    for row in input1:
        print(f"| {row[0]} | {row[1]} | {row[2]} | {row[3]} |")
    print("Output:")
    print("| BRANCH_ID | BRANCH_NAME | TOTAL_TRANSACTIONS | TOTAL_AMOUNT | REGION | LAST_AUDIT_DATE |")
    print("|-----------|-------------|--------------------|-------------|--------|-----------------|")
    for row in output1:
        print(f"| {row[0]} | {row[1]} | {row[2]} | {row[3]} | {row[4]} | {row[5]} |")
    print(f"Status: {status1}\n")
    print("### Scenario 2: Update\nInput:")
    print("| BRANCH_ID | REGION | LAST_AUDIT_DATE | IS_ACTIVE |")
    print("|-----------|--------|-----------------|-----------|")
    for row in input2:
        print(f"| {row[0]} | {row[1]} | {row[2]} | {row[3]} |")
    print("Output:")
    print("| BRANCH_ID | BRANCH_NAME | TOTAL_TRANSACTIONS | TOTAL_AMOUNT | REGION | LAST_AUDIT_DATE |")
    print("|-----------|-------------|--------------------|-------------|--------|-----------------|")
    for row in output2:
        print(f"| {row[0]} | {row[1]} | {row[2]} | {row[3]} | {row[4]} | {row[5]} |")
    print(f"Status: {status2}\n")
    spark.stop()
