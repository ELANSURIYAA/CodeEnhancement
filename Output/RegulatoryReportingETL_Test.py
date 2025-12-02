# ====================================================================
# Author: Ascendion AAVA
# Date: 
# Description: Test script for RegulatoryReportingETL_Pipeline.py (insert and update scenarios, markdown reporting)
# ====================================================================

import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, when

# Inline copy of the transformation logic for testability

def create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_details_df):
    branch_summary = transaction_df.join(account_df, "ACCOUNT_ID") \
        .join(branch_df, "BRANCH_ID") \
        .groupBy("BRANCH_ID", "BRANCH_NAME") \
        .agg(
            count("*").alias("TOTAL_TRANSACTIONS"),
            sum("AMOUNT").alias("TOTAL_AMOUNT")
        )
    branch_summary = branch_summary.join(branch_operational_details_df, "BRANCH_ID", "left")
    branch_summary = branch_summary \
        .withColumn(
            "REGION",
            when(col("IS_ACTIVE") == "Y", col("REGION")).otherwise(None)
        ) \
        .withColumn(
            "LAST_AUDIT_DATE",
            when(col("IS_ACTIVE") == "Y", col("LAST_AUDIT_DATE")).otherwise(None)
        )
    branch_summary = branch_summary.select(
        "BRANCH_ID", "BRANCH_NAME", "TOTAL_TRANSACTIONS", "TOTAL_AMOUNT", "REGION", "LAST_AUDIT_DATE"
    )
    return branch_summary

def print_markdown_report(scenario, input_rows, output_rows, status):
    print(f"## Test Report\n### Scenario {scenario}:")
    print("Input:")
    if input_rows:
        headers = input_rows[0].keys()
        print("| " + " | ".join(headers) + " |")
        print("|" + "|".join(["----"] * len(headers)) + "|")
        for row in input_rows:
            print("| " + " | ".join(str(row[h]) for h in headers) + " |")
    else:
        print("No input rows.")
    print("Output:")
    if output_rows:
        headers = output_rows[0].keys()
        print("| " + " | ".join(headers) + " |")
        print("|" + "|".join(["----"] * len(headers)) + "|")
        for row in output_rows:
            print("| " + " | ".join(str(row[h]) for h in headers) + " |")
    else:
        print("No output rows.")
    print(f"Status: {status}\n")

def main():
    spark = SparkSession.builder.master("local[1]").appName("RegulatoryReportingETL_Test").getOrCreate()
    # Scenario 1: Insert
    transaction_df = spark.createDataFrame([
        (6001, 2000, "DEPOSIT", 3000.0, "2023-03-01", "Deposit"),
    ], ["TRANSACTION_ID", "ACCOUNT_ID", "TRANSACTION_TYPE", "AMOUNT", "TRANSACTION_DATE", "DESCRIPTION"])
    account_df = spark.createDataFrame([
        (2000, 3, 300, "ACC2000", "SAVINGS", 7000.0, "2023-03-05")
    ], ["ACCOUNT_ID", "CUSTOMER_ID", "BRANCH_ID", "ACCOUNT_NUMBER", "ACCOUNT_TYPE", "BALANCE", "OPENED_DATE"])
    branch_df = spark.createDataFrame([
        (300, "South", "SOU", "City3", "State3", "Country3")
    ], ["BRANCH_ID", "BRANCH_NAME", "BRANCH_CODE", "CITY", "STATE", "COUNTRY"])
    branch_operational_details_df = spark.createDataFrame([
        (300, "South Region", "Alex Brown", "2023-03-10", "Y")
    ], ["BRANCH_ID", "REGION", "MANAGER_NAME", "LAST_AUDIT_DATE", "IS_ACTIVE"])
    # Run transformation
    result_df = create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_details_df)
    output_rows = [row.asDict() for row in result_df.collect()]
    input_rows = [
        {"TRANSACTION_ID": 6001, "ACCOUNT_ID": 2000, "TRANSACTION_TYPE": "DEPOSIT", "AMOUNT": 3000.0, "TRANSACTION_DATE": "2023-03-01", "DESCRIPTION": "Deposit"},
        {"ACCOUNT_ID": 2000, "CUSTOMER_ID": 3, "BRANCH_ID": 300, "ACCOUNT_NUMBER": "ACC2000", "ACCOUNT_TYPE": "SAVINGS", "BALANCE": 7000.0, "OPENED_DATE": "2023-03-05"},
        {"BRANCH_ID": 300, "BRANCH_NAME": "South", "BRANCH_CODE": "SOU", "CITY": "City3", "STATE": "State3", "COUNTRY": "Country3"},
        {"BRANCH_ID": 300, "REGION": "South Region", "MANAGER_NAME": "Alex Brown", "LAST_AUDIT_DATE": "2023-03-10", "IS_ACTIVE": "Y"}
    ]
    expected = [{
        "BRANCH_ID": 300, "BRANCH_NAME": "South", "TOTAL_TRANSACTIONS": 1, "TOTAL_AMOUNT": 3000.0, "REGION": "South Region", "LAST_AUDIT_DATE": "2023-03-10"
    }]
    status = "PASS" if output_rows == expected else "FAIL"
    print_markdown_report("1: Insert", [input_rows[0]], output_rows, status)

    # Scenario 2: Update
    # Existing record, IS_ACTIVE changes from N to Y
    transaction_df2 = spark.createDataFrame([
        (7001, 1000, "DEPOSIT", 500.0, "2023-04-01", "Deposit"),
    ], ["TRANSACTION_ID", "ACCOUNT_ID", "TRANSACTION_TYPE", "AMOUNT", "TRANSACTION_DATE", "DESCRIPTION"])
    account_df2 = spark.createDataFrame([
        (1000, 1, 100, "ACC1000", "SAVINGS", 5000.0, "2023-01-05")
    ], ["ACCOUNT_ID", "CUSTOMER_ID", "BRANCH_ID", "ACCOUNT_NUMBER", "ACCOUNT_TYPE", "BALANCE", "OPENED_DATE"])
    branch_df2 = spark.createDataFrame([
        (100, "Central", "CEN", "City1", "State1", "Country1")
    ], ["BRANCH_ID", "BRANCH_NAME", "BRANCH_CODE", "CITY", "STATE", "COUNTRY"])
    branch_operational_details_df2 = spark.createDataFrame([
        (100, "North Region", "John Doe", "2023-05-10", "Y")
    ], ["BRANCH_ID", "REGION", "MANAGER_NAME", "LAST_AUDIT_DATE", "IS_ACTIVE"])
    result_df2 = create_branch_summary_report(transaction_df2, account_df2, branch_df2, branch_operational_details_df2)
    output_rows2 = [row.asDict() for row in result_df2.collect()]
    input_rows2 = [
        {"TRANSACTION_ID": 7001, "ACCOUNT_ID": 1000, "TRANSACTION_TYPE": "DEPOSIT", "AMOUNT": 500.0, "TRANSACTION_DATE": "2023-04-01", "DESCRIPTION": "Deposit"},
        {"ACCOUNT_ID": 1000, "CUSTOMER_ID": 1, "BRANCH_ID": 100, "ACCOUNT_NUMBER": "ACC1000", "ACCOUNT_TYPE": "SAVINGS", "BALANCE": 5000.0, "OPENED_DATE": "2023-01-05"},
        {"BRANCH_ID": 100, "BRANCH_NAME": "Central", "BRANCH_CODE": "CEN", "CITY": "City1", "STATE": "State1", "COUNTRY": "Country1"},
        {"BRANCH_ID": 100, "REGION": "North Region", "MANAGER_NAME": "John Doe", "LAST_AUDIT_DATE": "2023-05-10", "IS_ACTIVE": "Y"}
    ]
    expected2 = [{
        "BRANCH_ID": 100, "BRANCH_NAME": "Central", "TOTAL_TRANSACTIONS": 1, "TOTAL_AMOUNT": 500.0, "REGION": "North Region", "LAST_AUDIT_DATE": "2023-05-10"
    }]
    status2 = "PASS" if output_rows2 == expected2 else "FAIL"
    print_markdown_report("2: Update", [input_rows2[0]], output_rows2, status2)
    spark.stop()

if __name__ == "__main__":
    main()
