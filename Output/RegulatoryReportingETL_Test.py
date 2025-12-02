#====================================================================
# Author: Ascendion AAVA
# Date:
# Description: Python-based test script for RegulatoryReportingETL_Pipeline.py covering insert and update scenarios. Generates markdown report for results.
#====================================================================

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Import pipeline functions
from Output.RegulatoryReportingETL_Pipeline import (
    create_sample_customer_df,
    create_sample_branch_df,
    create_sample_account_df,
    create_sample_transaction_df,
    create_sample_branch_operational_details_df,
    create_branch_summary_report
)

def run_insert_scenario(spark):
    # Scenario 1: Insert new branch operational details
    transaction_df = create_sample_transaction_df(spark)
    account_df = create_sample_account_df(spark)
    branch_df = create_sample_branch_df(spark)
    # New operational details (all IS_ACTIVE = 'Y')
    data = [
        (101, "North", "John Doe", "2023-03-01", "Y"),
        (102, "West", "Jane Smith", "2023-03-05", "Y")
    ]
    columns = ["BRANCH_ID", "REGION", "MANAGER_NAME", "LAST_AUDIT_DATE", "IS_ACTIVE"]
    branch_op_df = spark.createDataFrame(data, columns)
    output_df = create_branch_summary_report(transaction_df, account_df, branch_df, branch_op_df)
    return branch_op_df, output_df

def run_update_scenario(spark):
    # Scenario 2: Update existing branch operational details (change IS_ACTIVE for 102)
    transaction_df = create_sample_transaction_df(spark)
    account_df = create_sample_account_df(spark)
    branch_df = create_sample_branch_df(spark)
    # Updated details (BRANCH_ID 102 IS_ACTIVE = 'N')
    data = [
        (101, "North", "John Doe", "2023-03-01", "Y"),
        (102, "West", "Jane Smith", "2023-03-05", "N")
    ]
    columns = ["BRANCH_ID", "REGION", "MANAGER_NAME", "LAST_AUDIT_DATE", "IS_ACTIVE"]
    branch_op_df = spark.createDataFrame(data, columns)
    output_df = create_branch_summary_report(transaction_df, account_df, branch_df, branch_op_df)
    return branch_op_df, output_df

def df_to_markdown(df, columns):
    rows = df.select(*columns).collect()
    md = "| " + " | ".join(columns) + " |\n"
    md += "|" + "----|" * len(columns) + "\n"
    for row in rows:
        md += "| " + " | ".join([str(row[c]) if row[c] is not None else "" for c in columns]) + " |\n"
    return md

def validate_insert(branch_op_df, output_df):
    # Both branches should have REGION and LAST_AUDIT_DATE populated
    result = output_df.select("BRANCH_ID", "REGION", "LAST_AUDIT_DATE").collect()
    status = "PASS"
    for row in result:
        if row["BRANCH_ID"] in [101, 102]:
            if not row["REGION"] or not row["LAST_AUDIT_DATE"]:
                status = "FAIL"
    return status

def validate_update(branch_op_df, output_df):
    # BRANCH_ID 101 should have REGION/LAST_AUDIT_DATE, 102 should be None
    result = output_df.select("BRANCH_ID", "REGION", "LAST_AUDIT_DATE").collect()
    status = "PASS"
    for row in result:
        if row["BRANCH_ID"] == 101:
            if not row["REGION"] or not row["LAST_AUDIT_DATE"]:
                status = "FAIL"
        if row["BRANCH_ID"] == 102:
            if row["REGION"] or row["LAST_AUDIT_DATE"]:
                status = "FAIL"
    return status

def main():
    spark = SparkSession.builder.master("local[*]").appName("RegulatoryReportingETLTest").getOrCreate()
    # Scenario 1: Insert
    branch_op_df, output_df = run_insert_scenario(spark)
    input_md = df_to_markdown(branch_op_df, ["BRANCH_ID", "REGION", "LAST_AUDIT_DATE", "IS_ACTIVE"])
    output_md = df_to_markdown(output_df, ["BRANCH_ID", "BRANCH_NAME", "REGION", "LAST_AUDIT_DATE"])
    status = validate_insert(branch_op_df, output_df)
    print("## Test Report\n### Scenario 1: Insert")
    print("Input:")
    print(input_md)
    print("Output:")
    print(output_md)
    print(f"Status: {status}\n")
    # Scenario 2: Update
    branch_op_df, output_df = run_update_scenario(spark)
    input_md = df_to_markdown(branch_op_df, ["BRANCH_ID", "REGION", "LAST_AUDIT_DATE", "IS_ACTIVE"])
    output_md = df_to_markdown(output_df, ["BRANCH_ID", "BRANCH_NAME", "REGION", "LAST_AUDIT_DATE"])
    status = validate_update(branch_op_df, output_df)
    print("### Scenario 2: Update")
    print("Input:")
    print(input_md)
    print("Output:")
    print(output_md)
    print(f"Status: {status}\n")
    spark.stop()

if __name__ == "__main__":
    main()

#====================================================================
# Summary of Updates:
# - Covers both insert and update scenarios for new/changed branch operational details.
# - Validates conditional population of REGION and LAST_AUDIT_DATE columns.
# - Markdown report generated for each scenario with PASS/FAIL status.
#====================================================================
