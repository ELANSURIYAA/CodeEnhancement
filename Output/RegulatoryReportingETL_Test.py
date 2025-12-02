====================================================================
Author: Ascendion AAVA
Date: 
Description: Python-based test script for RegulatoryReportingETL_Pipeline.py covering insert and update scenarios.
====================================================================

import sys
import types
from pyspark.sql import SparkSession
from RegulatoryReportingETL_Pipeline import create_sample_dataframes, create_branch_summary_report
from pyspark.sql.functions import col

# Helper function to pretty print DataFrame as markdown table
def df_to_markdown(df, columns=None):
    if columns is None:
        columns = df.columns
    rows = df.select(*columns).collect()
    header = '| ' + ' | '.join(columns) + ' |\n'
    sep = '| ' + ' | '.join(['----'] * len(columns)) + ' |\n'
    body = ''
    for r in rows:
        body += '| ' + ' | '.join(str(r[c]) if r[c] is not None else '' for c in columns) + ' |\n'
    return header + sep + body

# Test Scenario 1: Insert new branch summary rows
def test_insert():
    spark = SparkSession.builder.master("local[1]").appName("TestInsert").getOrCreate()
    customer_df, account_df, transaction_df, branch_df, branch_operational_df = create_sample_dataframes(spark)
    # All data is new, so all rows should be inserted
    result_df = create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_df)
    expected_rows = [
        (10, "Central", 1, 500.0, "North", "2023-03-01"),
        (11, "West", 1, 300.0, None, None)
    ]
    # Validate output
    actual_rows = [(r['BRANCH_ID'], r['BRANCH_NAME'], r['TOTAL_TRANSACTIONS'], float(r['TOTAL_AMOUNT']), r['REGION'], r['LAST_AUDIT_DATE']) for r in result_df.collect()]
    status = 'PASS' if actual_rows == expected_rows else 'FAIL'
    input_md = '| BRANCH_ID | BRANCH_NAME | TOTAL_TRANSACTIONS | TOTAL_AMOUNT | REGION | LAST_AUDIT_DATE |\n|----|-------|----|----|----|----|\n| 10 | Central | 1 | 500.0 | North | 2023-03-01 |\n| 11 | West | 1 | 300.0 |  |  |\n'
    output_md = df_to_markdown(result_df, ['BRANCH_ID', 'BRANCH_NAME', 'TOTAL_TRANSACTIONS', 'TOTAL_AMOUNT', 'REGION', 'LAST_AUDIT_DATE'])
    spark.stop()
    return {
        'scenario': 'Insert',
        'input': input_md,
        'output': output_md,
        'status': status
    }

# Test Scenario 2: Update branch summary rows
def test_update():
    spark = SparkSession.builder.master("local[1]").appName("TestUpdate").getOrCreate()
    customer_df, account_df, transaction_df, branch_df, branch_operational_df = create_sample_dataframes(spark)
    # Simulate update: Change operational details for BRANCH_ID 10 to inactive and region to 'East'
    updated_branch_operational_data = [
        (10, "East", "John Doe", "2023-04-01", "N"),
        (11, "South", "Jane Smith", "2023-03-15", "Y")
    ]
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType
    branch_operational_schema = StructType([
        StructField("BRANCH_ID", IntegerType()),
        StructField("REGION", StringType()),
        StructField("MANAGER_NAME", StringType()),
        StructField("LAST_AUDIT_DATE", StringType()),
        StructField("IS_ACTIVE", StringType())
    ])
    branch_operational_df = spark.createDataFrame(updated_branch_operational_data, schema=branch_operational_schema)
    result_df = create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_df)
    expected_rows = [
        (10, "Central", 1, 500.0, None, None),
        (11, "West", 1, 300.0, "South", "2023-03-15")
    ]
    actual_rows = [(r['BRANCH_ID'], r['BRANCH_NAME'], r['TOTAL_TRANSACTIONS'], float(r['TOTAL_AMOUNT']), r['REGION'], r['LAST_AUDIT_DATE']) for r in result_df.collect()]
    status = 'PASS' if actual_rows == expected_rows else 'FAIL'
    input_md = '| BRANCH_ID | BRANCH_NAME | TOTAL_TRANSACTIONS | TOTAL_AMOUNT | REGION | LAST_AUDIT_DATE |\n|----|-------|----|----|----|----|\n| 10 | Central | 1 | 500.0 |  |  |\n| 11 | West | 1 | 300.0 | South | 2023-03-15 |\n'
    output_md = df_to_markdown(result_df, ['BRANCH_ID', 'BRANCH_NAME', 'TOTAL_TRANSACTIONS', 'TOTAL_AMOUNT', 'REGION', 'LAST_AUDIT_DATE'])
    spark.stop()
    return {
        'scenario': 'Update',
        'input': input_md,
        'output': output_md,
        'status': status
    }

# Main test runner
def main():
    report = '## Test Report\n'
    # Scenario 1: Insert
    res_insert = test_insert()
    report += f"### Scenario 1: {res_insert['scenario']}\nInput:\n{res_insert['input']}Output:\n{res_insert['output']}Status: {res_insert['status']}\n\n"
    # Scenario 2: Update
    res_update = test_update()
    report += f"### Scenario 2: {res_update['scenario']}\nInput:\n{res_update['input']}Output:\n{res_update['output']}Status: {res_update['status']}\n\n"
    print(report)

if __name__ == "__main__":
    main()

# ====================================================================
# Summary of Updation in this Version
# - Added two test scenarios: Insert and Update for branch summary report logic
# - Validates conditional population of REGION and LAST_AUDIT_DATE columns
# - Produces markdown report with input/output/results for audit
# ====================================================================
