====================================================================
Author: Ascendion AAVA
Date: 
Description: Python-based test script for RegulatoryReportingETL_Pipeline.py covering insert and update scenarios. Outputs markdown report.
====================================================================

import sys
import io
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from pyspark.sql.functions import col, count, sum, when

def get_spark_session():
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.appName("RegulatoryReportingETLTest").getOrCreate()
    return spark

def create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_df):
    base_df = transaction_df.join(account_df, "ACCOUNT_ID") \
                           .join(branch_df, "BRANCH_ID") \
                           .groupBy("BRANCH_ID", "BRANCH_NAME") \
                           .agg(
                               count("*").alias("TOTAL_TRANSACTIONS"),
                               sum("AMOUNT").alias("TOTAL_AMOUNT")
                           )
    result_df = base_df.join(branch_operational_df, "BRANCH_ID", "left")
    result_df = result_df.withColumn(
        "REGION", when(col("IS_ACTIVE") == "Y", col("REGION")).otherwise(None)
    ).withColumn(
        "LAST_AUDIT_DATE", when(col("IS_ACTIVE") == "Y", col("LAST_AUDIT_DATE")).otherwise(None)
    )
    result_df = result_df.select(
        "BRANCH_ID", "BRANCH_NAME", "TOTAL_TRANSACTIONS", "TOTAL_AMOUNT", "REGION", "LAST_AUDIT_DATE"
    )
    return result_df

def scenario_1_insert(spark):
    # Sample data: all branches are new
    branch_schema = StructType([
        StructField("BRANCH_ID", IntegerType(), True),
        StructField("BRANCH_NAME", StringType(), True)
    ])
    branch_data = [
        (201, "Central"),
        (202, "North")
    ]
    branch_df = spark.createDataFrame(branch_data, schema=branch_schema)

    account_schema = StructType([
        StructField("ACCOUNT_ID", IntegerType(), True),
        StructField("CUSTOMER_ID", IntegerType(), True),
        StructField("BRANCH_ID", IntegerType(), True)
    ])
    account_data = [
        (2001, 10, 201),
        (2002, 11, 202)
    ]
    account_df = spark.createDataFrame(account_data, schema=account_schema)

    transaction_schema = StructType([
        StructField("TRANSACTION_ID", IntegerType(), True),
        StructField("ACCOUNT_ID", IntegerType(), True),
        StructField("TRANSACTION_TYPE", StringType(), True),
        StructField("AMOUNT", DoubleType(), True),
        StructField("TRANSACTION_DATE", DateType(), True),
        StructField("DESCRIPTION", StringType(), True)
    ])
    transaction_data = [
        (6001, 2001, "Deposit", 3000.0, date(2023,7,1), "Initial deposit"),
        (6002, 2002, "Withdrawal", 1000.0, date(2023,7,2), "ATM withdrawal")
    ]
    transaction_df = spark.createDataFrame(transaction_data, schema=transaction_schema)

    branch_operational_schema = StructType([
        StructField("BRANCH_ID", IntegerType(), True),
        StructField("REGION", StringType(), True),
        StructField("MANAGER_NAME", StringType(), True),
        StructField("LAST_AUDIT_DATE", StringType(), True),
        StructField("IS_ACTIVE", StringType(), True)
    ])
    branch_operational_data = [
        (201, "South", "Jim Beam", "2023-07-10", "Y"),
        (202, "NorthEast", "Sara Lee", "2023-07-12", "Y")
    ]
    branch_operational_df = spark.createDataFrame(branch_operational_data, schema=branch_operational_schema)

    output_df = create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_df)
    return branch_df, transaction_df, output_df

def scenario_2_update(spark):
    # Sample data: branch already exists, update scenario
    branch_schema = StructType([
        StructField("BRANCH_ID", IntegerType(), True),
        StructField("BRANCH_NAME", StringType(), True)
    ])
    branch_data = [
        (101, "Downtown"),
        (102, "Uptown")
    ]
    branch_df = spark.createDataFrame(branch_data, schema=branch_schema)

    account_schema = StructType([
        StructField("ACCOUNT_ID", IntegerType(), True),
        StructField("CUSTOMER_ID", IntegerType(), True),
        StructField("BRANCH_ID", IntegerType(), True)
    ])
    account_data = [
        (1001, 1, 101),
        (1002, 2, 102)
    ]
    account_df = spark.createDataFrame(account_data, schema=account_schema)

    transaction_schema = StructType([
        StructField("TRANSACTION_ID", IntegerType(), True),
        StructField("ACCOUNT_ID", IntegerType(), True),
        StructField("TRANSACTION_TYPE", StringType(), True),
        StructField("AMOUNT", DoubleType(), True),
        StructField("TRANSACTION_DATE", DateType(), True),
        StructField("DESCRIPTION", StringType(), True)
    ])
    transaction_data = [
        (5001, 1001, "Deposit", 2000.0, date(2023,5,1), "Initial deposit"),
        (5002, 1002, "Withdrawal", 1500.0, date(2023,5,2), "ATM withdrawal")
    ]
    transaction_df = spark.createDataFrame(transaction_data, schema=transaction_schema)

    branch_operational_schema = StructType([
        StructField("BRANCH_ID", IntegerType(), True),
        StructField("REGION", StringType(), True),
        StructField("MANAGER_NAME", StringType(), True),
        StructField("LAST_AUDIT_DATE", StringType(), True),
        StructField("IS_ACTIVE", StringType(), True)
    ])
    branch_operational_data = [
        (101, "East", "John Doe", "2023-06-01", "Y"),
        (102, "West", "Jane Smith", "2023-06-15", "N")
    ]
    branch_operational_df = spark.createDataFrame(branch_operational_data, schema=branch_operational_schema)

    output_df = create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_df)
    return branch_df, transaction_df, output_df

def df_to_markdown(df, columns):
    data = df.select(*columns).collect()
    header = "| " + " | ".join(columns) + " |"
    sep = "|" + "----|" * len(columns)
    rows = ["| " + " | ".join([str(row[col]) if row[col] is not None else "" for col in columns]) + " |" for row in data]
    return "\n".join([header, sep] + rows)

def main():
    spark = get_spark_session()
    markdown = []
    # Scenario 1: Insert
    branch_df, transaction_df, output_df = scenario_1_insert(spark)
    markdown.append("## Test Report\n### Scenario 1: Insert")
    markdown.append("Input (Branches):")
    markdown.append(df_to_markdown(branch_df, ["BRANCH_ID", "BRANCH_NAME"]))
    markdown.append("Input (Transactions):")
    markdown.append(df_to_markdown(transaction_df, ["TRANSACTION_ID", "ACCOUNT_ID", "AMOUNT"]))
    markdown.append("Output:")
    markdown.append(df_to_markdown(output_df, ["BRANCH_ID", "BRANCH_NAME", "TOTAL_TRANSACTIONS", "TOTAL_AMOUNT", "REGION", "LAST_AUDIT_DATE"]))
    # Validate: Both REGION populated, LAST_AUDIT_DATE populated, correct totals
    status = "PASS"
    for row in output_df.collect():
        if not row[4] or not row[5]:
            status = "FAIL"
            break
    markdown.append(f"Status: {status}\n")

    # Scenario 2: Update
    branch_df, transaction_df, output_df = scenario_2_update(spark)
    markdown.append("### Scenario 2: Update")
    markdown.append("Input (Branches):")
    markdown.append(df_to_markdown(branch_df, ["BRANCH_ID", "BRANCH_NAME"]))
    markdown.append("Input (Transactions):")
    markdown.append(df_to_markdown(transaction_df, ["TRANSACTION_ID", "ACCOUNT_ID", "AMOUNT"]))
    markdown.append("Output:")
    markdown.append(df_to_markdown(output_df, ["BRANCH_ID", "BRANCH_NAME", "TOTAL_TRANSACTIONS", "TOTAL_AMOUNT", "REGION", "LAST_AUDIT_DATE"]))
    # Validate: REGION/LAST_AUDIT_DATE only for IS_ACTIVE='Y'
    status = "PASS"
    for row in output_df.collect():
        if row[0] == 101:
            if row[4] != "East" or row[5] != "2023-06-01":
                status = "FAIL"
                break
        if row[0] == 102:
            if row[4] is not None or row[5] is not None:
                status = "FAIL"
                break
    markdown.append(f"Status: {status}\n")

    # Print markdown report
    print("\n".join(markdown))
    spark.stop()

if __name__ == "__main__":
    main()
