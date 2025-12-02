====================================================================
Author: Ascendion AAVA
Date: 
Description: Python-based test script for RegulatoryReportingETL_Pipeline.py (insert/update scenarios)
====================================================================

import logging
from pyspark.sql import SparkSession
from RegulatoryReportingETL_Pipeline import create_sample_data, create_branch_summary_report
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col
import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_insert_scenario(spark):
    """
    Scenario 1: Insert new rows into branch_summary_report
    """
    # Sample branch_op_df: all branches are new
    branch_op_schema = StructType([
        StructField("BRANCH_ID", IntegerType()),
        StructField("REGION", StringType()),
        StructField("MANAGER_NAME", StringType()),
        StructField("LAST_AUDIT_DATE", StringType()),
        StructField("IS_ACTIVE", StringType())
    ])
    branch_op_data = [
        (101, "North", "John Doe", "2023-05-10", "Y"),
        (102, "South", "Jane Smith", "2023-05-11", "Y")
    ]
    branch_op_df = spark.createDataFrame(branch_op_data, branch_op_schema)

    # Other sample data
    customer_df, account_df, transaction_df, branch_df, _ = create_sample_data(spark)
    # Run ETL logic
    result_df = create_branch_summary_report(transaction_df, account_df, branch_df, branch_op_df)
    result = result_df.orderBy("BRANCH_ID").collect()
    return branch_op_data, [tuple([row.BRANCH_ID, row.BRANCH_NAME, row.REGION, row.LAST_AUDIT_DATE]) for row in result]

def run_update_scenario(spark):
    """
    Scenario 2: Update existing rows in branch_summary_report
    """
    # Sample branch_op_df: one branch is active, one is inactive
    branch_op_schema = StructType([
        StructField("BRANCH_ID", IntegerType()),
        StructField("REGION", StringType()),
        StructField("MANAGER_NAME", StringType()),
        StructField("LAST_AUDIT_DATE", StringType()),
        StructField("IS_ACTIVE", StringType())
    ])
    branch_op_data = [
        (101, "North", "John Doe", "2023-05-10", "Y"),
        (102, "South", "Jane Smith", "2023-05-11", "N")
    ]
    branch_op_df = spark.createDataFrame(branch_op_data, branch_op_schema)

    customer_df, account_df, transaction_df, branch_df, _ = create_sample_data(spark)
    result_df = create_branch_summary_report(transaction_df, account_df, branch_df, branch_op_df)
    result = result_df.orderBy("BRANCH_ID").collect()
    return branch_op_data, [tuple([row.BRANCH_ID, row.BRANCH_NAME, row.REGION, row.LAST_AUDIT_DATE]) for row in result]

def print_markdown_report(scenario_name, input_data, output_data, expected_output):
    print(f"## Test Report\n### {scenario_name}")
    print("Input:")
    print("| BRANCH_ID | REGION | MANAGER_NAME | LAST_AUDIT_DATE | IS_ACTIVE |")
    print("|-----------|--------|--------------|-----------------|-----------|")
    for row in input_data:
        print(f"| {row[0]} | {row[1]} | {row[2]} | {row[3]} | {row[4]} |")
    print("Output:")
    print("| BRANCH_ID | BRANCH_NAME | REGION | LAST_AUDIT_DATE |")
    print("|-----------|-------------|--------|-----------------|")
    for row in output_data:
        print(f"| {row[0]} | {row[1]} | {row[2] if row[2] is not None else ''} | {row[3] if row[3] is not None else ''} |")
    status = "PASS" if output_data == expected_output else "FAIL"
    print(f"Status: {status}\n")

def main():
    spark = SparkSession.builder.master("local[*]").appName("RegulatoryReportingETL_Test").getOrCreate()
    # Scenario 1: Insert
    input_data_1, output_data_1 = run_insert_scenario(spark)
    expected_output_1 = [
        (101, "Main Branch", "North", "2023-05-10"),
        (102, "Branch2", "South", "2023-05-11")
    ]
    print_markdown_report("Scenario 1: Insert", input_data_1, output_data_1, expected_output_1)
    # Scenario 2: Update
    input_data_2, output_data_2 = run_update_scenario(spark)
    expected_output_2 = [
        (101, "Main Branch", "North", "2023-05-10"),
        (102, "Branch2", None, None)
    ]
    print_markdown_report("Scenario 2: Update", input_data_2, output_data_2, expected_output_2)
    spark.stop()

if __name__ == "__main__":
    main()
