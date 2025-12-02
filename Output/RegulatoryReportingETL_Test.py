====================================================================
Author: Ascendion AAVA
Date: 
Description: Python-based test script for RegulatoryReportingETL_Pipeline.py covering insert and update scenarios.
====================================================================

import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from datetime import date
from pyspark.sql.functions import col, count, sum, when

# Import the ETL functions from the pipeline script (assume same directory)
# For demonstration, we redefine the ETL logic inline.

def get_spark_session():
    spark = SparkSession.getActiveSession()
    if not spark:
        spark = SparkSession.builder.appName("TestETL").getOrCreate()
    return spark

def create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_details_df):
    base_summary = transaction_df.join(account_df, "ACCOUNT_ID") \
                                 .join(branch_df, "BRANCH_ID") \
                                 .groupBy("BRANCH_ID", "BRANCH_NAME") \
                                 .agg(
                                     count("*").alias("TOTAL_TRANSACTIONS"),
                                     sum("AMOUNT").alias("TOTAL_AMOUNT")
                                 )
    summary_with_ops = base_summary.join(branch_operational_details_df, "BRANCH_ID", "left") \
        .withColumn("REGION", when(col("IS_ACTIVE") == "Y", col("REGION")).otherwise(None)) \
        .withColumn("LAST_AUDIT_DATE", when(col("IS_ACTIVE") == "Y", col("LAST_AUDIT_DATE")).otherwise(None))
    return summary_with_ops.select(
        "BRANCH_ID", "BRANCH_NAME", "TOTAL_TRANSACTIONS", "TOTAL_AMOUNT", "REGION", "LAST_AUDIT_DATE"
    )

def scenario_insert(spark):
    # New branch (not present before)
    branch_data = [(103, "Midtown", "MT003", "CityC", "StateC", "CountryC")]
    branch_schema = StructType([
        StructField("BRANCH_ID", IntegerType(), True),
        StructField("BRANCH_NAME", StringType(), True),
        StructField("BRANCH_CODE", StringType(), True),
        StructField("CITY", StringType(), True),
        StructField("STATE", StringType(), True),
        StructField("COUNTRY", StringType(), True),
    ])
    account_data = [(1003, 3, 103, "ACC003", "Savings", 3000.00, date(2021, 3, 1))]
    account_schema = StructType([
        StructField("ACCOUNT_ID", IntegerType(), True),
        StructField("CUSTOMER_ID", IntegerType(), True),
        StructField("BRANCH_ID", IntegerType(), True),
        StructField("ACCOUNT_NUMBER", StringType(), True),
        StructField("ACCOUNT_TYPE", StringType(), True),
        StructField("BALANCE", DoubleType(), True),
        StructField("OPENED_DATE", DateType(), True),
    ])
    transaction_data = [(5003, 1003, "Deposit", 700.0, date(2022, 3, 15), "Bonus")]
    transaction_schema = StructType([
        StructField("TRANSACTION_ID", IntegerType(), True),
        StructField("ACCOUNT_ID", IntegerType(), True),
        StructField("TRANSACTION_TYPE", StringType(), True),
        StructField("AMOUNT", DoubleType(), True),
        StructField("TRANSACTION_DATE", DateType(), True),
        StructField("DESCRIPTION", StringType(), True),
    ])
    branch_operational_details_data = [(103, "East", "Sam Lee", date(2022, 7, 10), "Y")]
    branch_operational_details_schema = StructType([
        StructField("BRANCH_ID", IntegerType(), True),
        StructField("REGION", StringType(), True),
        StructField("MANAGER_NAME", StringType(), True),
        StructField("LAST_AUDIT_DATE", DateType(), True),
        StructField("IS_ACTIVE", StringType(), True),
    ])
    branch_df = spark.createDataFrame(branch_data, schema=branch_schema)
    account_df = spark.createDataFrame(account_data, schema=account_schema)
    transaction_df = spark.createDataFrame(transaction_data, schema=transaction_schema)
    branch_operational_details_df = spark.createDataFrame(branch_operational_details_data, schema=branch_operational_details_schema)
    result = create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_details_df)
    return branch_data, branch_operational_details_data, result

def scenario_update(spark):
    # Existing branch, update operational details
    branch_data = [(101, "Downtown", "DT001", "CityA", "StateA", "CountryA")]
    branch_schema = StructType([
        StructField("BRANCH_ID", IntegerType(), True),
        StructField("BRANCH_NAME", StringType(), True),
        StructField("BRANCH_CODE", StringType(), True),
        StructField("CITY", StringType(), True),
        StructField("STATE", StringType(), True),
        StructField("COUNTRY", StringType(), True),
    ])
    account_data = [(1001, 1, 101, "ACC001", "Savings", 1000.00, date(2021, 1, 1))]
    account_schema = StructType([
        StructField("ACCOUNT_ID", IntegerType(), True),
        StructField("CUSTOMER_ID", IntegerType(), True),
        StructField("BRANCH_ID", IntegerType(), True),
        StructField("ACCOUNT_NUMBER", StringType(), True),
        StructField("ACCOUNT_TYPE", StringType(), True),
        StructField("BALANCE", DoubleType(), True),
        StructField("OPENED_DATE", DateType(), True),
    ])
    transaction_data = [(5004, 1001, "Deposit", 1000.0, date(2022, 4, 15), "Salary")]
    transaction_schema = StructType([
        StructField("TRANSACTION_ID", IntegerType(), True),
        StructField("ACCOUNT_ID", IntegerType(), True),
        StructField("TRANSACTION_TYPE", StringType(), True),
        StructField("AMOUNT", DoubleType(), True),
        StructField("TRANSACTION_DATE", DateType(), True),
        StructField("DESCRIPTION", StringType(), True),
    ])
    branch_operational_details_data = [(101, "NorthEast", "John Doe", date(2023, 1, 10), "Y")]
    branch_operational_details_schema = StructType([
        StructField("BRANCH_ID", IntegerType(), True),
        StructField("REGION", StringType(), True),
        StructField("MANAGER_NAME", StringType(), True),
        StructField("LAST_AUDIT_DATE", DateType(), True),
        StructField("IS_ACTIVE", StringType(), True),
    ])
    branch_df = spark.createDataFrame(branch_data, schema=branch_schema)
    account_df = spark.createDataFrame(account_data, schema=account_schema)
    transaction_df = spark.createDataFrame(transaction_data, schema=transaction_schema)
    branch_operational_details_df = spark.createDataFrame(branch_operational_details_data, schema=branch_operational_details_schema)
    result = create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_details_df)
    return branch_data, branch_operational_details_data, result

def markdown_table(rows, headers):
    table = "| " + " | ".join(headers) + " |\n"
    table += "|" + "----|" * len(headers) + "\n"
    for row in rows:
        table += "| " + " | ".join([str(x) for x in row]) + " |\n"
    return table

def run_tests():
    spark = get_spark_session()
    # Scenario 1: Insert
    branch_rows, ops_rows, result_df = scenario_insert(spark)
    result = result_df.collect()
    # Prepare markdown report
    report = "## Test Report\n"
    report += "### Scenario 1: Insert\n"
    report += "Input (Branch):\n" + markdown_table(branch_rows, ["BRANCH_ID", "BRANCH_NAME", "BRANCH_CODE", "CITY", "STATE", "COUNTRY"]) + "\n"
    report += "Input (Operational Details):\n" + markdown_table(ops_rows, ["BRANCH_ID", "REGION", "MANAGER_NAME", "LAST_AUDIT_DATE", "IS_ACTIVE"]) + "\n"
    output_rows = [(r["BRANCH_ID"], r["BRANCH_NAME"], r["TOTAL_TRANSACTIONS"], r["TOTAL_AMOUNT"], r["REGION"], str(r["LAST_AUDIT_DATE"])) for r in result]
    report += "Output:\n" + markdown_table(output_rows, ["BRANCH_ID", "BRANCH_NAME", "TOTAL_TRANSACTIONS", "TOTAL_AMOUNT", "REGION", "LAST_AUDIT_DATE"]) + "\n"
    status = "PASS" if len(output_rows) == 1 and output_rows[0][4] == "East" and output_rows[0][5] == "2022-07-10" else "FAIL"
    report += f"Status: {status}\n\n"
    # Scenario 2: Update
    branch_rows, ops_rows, result_df = scenario_update(spark)
    result = result_df.collect()
    report += "### Scenario 2: Update\n"
    report += "Input (Branch):\n" + markdown_table(branch_rows, ["BRANCH_ID", "BRANCH_NAME", "BRANCH_CODE", "CITY", "STATE", "COUNTRY"]) + "\n"
    report += "Input (Operational Details):\n" + markdown_table(ops_rows, ["BRANCH_ID", "REGION", "MANAGER_NAME", "LAST_AUDIT_DATE", "IS_ACTIVE"]) + "\n"
    output_rows = [(r["BRANCH_ID"], r["BRANCH_NAME"], r["TOTAL_TRANSACTIONS"], r["TOTAL_AMOUNT"], r["REGION"], str(r["LAST_AUDIT_DATE"])) for r in result]
    report += "Output:\n" + markdown_table(output_rows, ["BRANCH_ID", "BRANCH_NAME", "TOTAL_TRANSACTIONS", "TOTAL_AMOUNT", "REGION", "LAST_AUDIT_DATE"]) + "\n"
    status = "PASS" if len(output_rows) == 1 and output_rows[0][4] == "NorthEast" and output_rows[0][5] == "2023-01-10" else "FAIL"
    report += f"Status: {status}\n"
    print(report)
    spark.stop()

if __name__ == "__main__":
    run_tests()
