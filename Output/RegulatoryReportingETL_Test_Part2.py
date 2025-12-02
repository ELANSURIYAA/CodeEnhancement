def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, 
                               branch_df: DataFrame, branch_operational_df: DataFrame) -> DataFrame:
    """
    Creates the BRANCH_SUMMARY_REPORT DataFrame (same logic as in pipeline)
    """
    logger.info("Creating Enhanced Branch Summary Report DataFrame for testing.")
    
    # Original aggregation logic
    base_summary = transaction_df.join(account_df, "ACCOUNT_ID") \
                                .join(branch_df, "BRANCH_ID") \
                                .groupBy("BRANCH_ID", "BRANCH_NAME") \
                                .agg(
                                    count("*").alias("TOTAL_TRANSACTIONS"),
                                    spark_sum("AMOUNT").alias("TOTAL_AMOUNT")
                                )
    
    # Integration with BRANCH_OPERATIONAL_DETAILS
    enhanced_summary = base_summary.join(
        branch_operational_df, 
        base_summary.BRANCH_ID == branch_operational_df.BRANCH_ID, 
        "left"
    ).select(
        base_summary.BRANCH_ID,
        base_summary.BRANCH_NAME,
        base_summary.TOTAL_TRANSACTIONS,
        base_summary.TOTAL_AMOUNT,
        when(branch_operational_df.IS_ACTIVE == "Y", branch_operational_df.REGION)
        .otherwise(lit(None)).alias("REGION"),
        when(branch_operational_df.IS_ACTIVE == "Y", branch_operational_df.LAST_AUDIT_DATE)
        .otherwise(lit(None)).alias("LAST_AUDIT_DATE")
    )
    
    return enhanced_summary

def run_test_scenario_1(spark: SparkSession) -> TestResult:
    """
    Test Scenario 1: Insert new records into target table
    """
    logger.info("Running Test Scenario 1: Insert")
    result = TestResult("Scenario 1: Insert")
    
    try:
        # Create test data
        branch_df, account_df, transaction_df, branch_operational_df = create_test_scenario_1_data(spark)
        
        # Record input data for reporting
        input_data = []
        branch_data = branch_df.collect()
        for row in branch_data:
            input_data.append({
                "BRANCH_ID": row.BRANCH_ID,
                "BRANCH_NAME": row.BRANCH_NAME,
                "REGION": None,  # Will be populated from operational details
                "LAST_AUDIT_DATE": None
            })
        
        # Add operational details to input
        operational_data = branch_operational_df.collect()
        for i, op_row in enumerate(operational_data):
            if i < len(input_data):
                input_data[i]["REGION"] = op_row.REGION if op_row.IS_ACTIVE == "Y" else None
                input_data[i]["LAST_AUDIT_DATE"] = str(op_row.LAST_AUDIT_DATE) if op_row.IS_ACTIVE == "Y" else None
        
        result.input_data = input_data
        
        # Execute the ETL logic
        output_df = create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_df)
        
        # Collect output data
        output_data = []
        output_rows = output_df.collect()
        for row in output_rows:
            output_data.append({
                "BRANCH_ID": row.BRANCH_ID,
                "BRANCH_NAME": row.BRANCH_NAME,
                "TOTAL_TRANSACTIONS": row.TOTAL_TRANSACTIONS,
                "TOTAL_AMOUNT": row.TOTAL_AMOUNT,
                "REGION": row.REGION,
                "LAST_AUDIT_DATE": str(row.LAST_AUDIT_DATE) if row.LAST_AUDIT_DATE else None
            })
        
        result.output_data = output_data
        
        # Validation: Check if new records were inserted
        expected_branch_ids = [103, 104]
        actual_branch_ids = [row["BRANCH_ID"] for row in output_data]
        
        if all(branch_id in actual_branch_ids for branch_id in expected_branch_ids):
            # Check if REGION and LAST_AUDIT_DATE are populated correctly
            valid_data = True
            for row in output_data:
                if row["BRANCH_ID"] in expected_branch_ids:
                    if row["REGION"] is None or row["LAST_AUDIT_DATE"] is None:
                        valid_data = False
                        break
            
            if valid_data:
                result.status = "PASS"
                result.details = f"Successfully inserted {len(expected_branch_ids)} new records with operational details"
            else:
                result.status = "FAIL"
                result.details = "New records inserted but operational details not populated correctly"
        else:
            result.status = "FAIL"
            result.details = f"Expected branch IDs {expected_branch_ids} not found in output"
            
    except Exception as e:
        result.status = "FAIL"
        result.details = f"Test failed with exception: {str(e)}"
        logger.error(f"Test Scenario 1 failed: {e}")
    
    return result

def run_test_scenario_2(spark: SparkSession) -> TestResult:
    """
    Test Scenario 2: Update existing records in target table
    """
    logger.info("Running Test Scenario 2: Update")
    result = TestResult("Scenario 2: Update")
    
    try:
        # Create base data (existing records)
        base_df = create_base_data(spark)
        
        # Create test data for updates
        branch_df, account_df, transaction_df, branch_operational_df = create_test_scenario_2_data(spark)
        
        # Record input data for reporting
        input_data = []
        branch_data = branch_df.collect()
        operational_data = branch_operational_df.collect()
        
        for i, branch_row in enumerate(branch_data):
            op_row = operational_data[i] if i < len(operational_data) else None
            input_data.append({
                "BRANCH_ID": branch_row.BRANCH_ID,
                "BRANCH_NAME": branch_row.BRANCH_NAME,
                "REGION": op_row.REGION if op_row and op_row.IS_ACTIVE == "Y" else None,
                "LAST_AUDIT_DATE": str(op_row.LAST_AUDIT_DATE) if op_row and op_row.IS_ACTIVE == "Y" else None
            })
        
        result.input_data = input_data
        
        # Execute the ETL logic
        output_df = create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_df)
        
        # Collect output data
        output_data = []
        output_rows = output_df.collect()
        for row in output_rows:
            output_data.append({
                "BRANCH_ID": row.BRANCH_ID,
                "BRANCH_NAME": row.BRANCH_NAME,
                "TOTAL_TRANSACTIONS": row.TOTAL_TRANSACTIONS,
                "TOTAL_AMOUNT": row.TOTAL_AMOUNT,
                "REGION": row.REGION,
                "LAST_AUDIT_DATE": str(row.LAST_AUDIT_DATE) if row.LAST_AUDIT_DATE else None
            })
        
        result.output_data = output_data
        
        # Validation: Check if existing records were updated
        expected_branch_ids = [101, 102]
        actual_branch_ids = [row["BRANCH_ID"] for row in output_data]
        
        if all(branch_id in actual_branch_ids for branch_id in expected_branch_ids):
            # Check if operational details were updated
            updated_correctly = True
            for row in output_data:
                if row["BRANCH_ID"] == 101:
                    if row["REGION"] != "Northeast Updated":
                        updated_correctly = False
                        break
                elif row["BRANCH_ID"] == 102:
                    if row["REGION"] != "West Coast Updated":
                        updated_correctly = False
                        break
            
            if updated_correctly:
                result.status = "PASS"
                result.details = f"Successfully updated {len(expected_branch_ids)} existing records with new operational details"
            else:
                result.status = "FAIL"
                result.details = "Existing records found but operational details not updated correctly"
        else:
            result.status = "FAIL"
            result.details = f"Expected branch IDs {expected_branch_ids} not found in output"
            
    except Exception as e:
        result.status = "FAIL"
        result.details = f"Test failed with exception: {str(e)}"
        logger.error(f"Test Scenario 2 failed: {e}")
    
    return result

def generate_markdown_report(test_results: list) -> str:
    """
    Generates a markdown report for test results
    """
    report = "# Test Report\n\n"
    report += "## PySpark ETL Pipeline Test Results\n\n"
    report += "### Test Summary\n"
    
    passed_tests = sum(1 for result in test_results if result.status == "PASS")
    total_tests = len(test_results)
    
    report += f"- **Total Tests:** {total_tests}\n"
    report += f"- **Passed:** {passed_tests}\n"
    report += f"- **Failed:** {total_tests - passed_tests}\n\n"
    
    for result in test_results:
        report += f"### {result.scenario_name}\n\n"
        
        # Input section
        report += "**Input:**\n\n"
        if result.input_data:
            report += "| BRANCH_ID | BRANCH_NAME | REGION | LAST_AUDIT_DATE |\n"
            report += "|-----------|-------------|--------|-----------------|\n"
            for row in result.input_data:
                region = row.get("REGION", "N/A") or "N/A"
                audit_date = row.get("LAST_AUDIT_DATE", "N/A") or "N/A"
                report += f"| {row['BRANCH_ID']} | {row['BRANCH_NAME']} | {region} | {audit_date} |\n"
        else:
            report += "No input data available\n"
        
        report += "\n"
        
        # Output section
        report += "**Output:**\n\n"
        if result.output_data:
            report += "| BRANCH_ID | BRANCH_NAME | TOTAL_TRANSACTIONS | TOTAL_AMOUNT | REGION | LAST_AUDIT_DATE |\n"
            report += "|-----------|-------------|-------------------|--------------|--------|-----------------|\n"
            for row in result.output_data:
                region = row.get("REGION", "N/A") or "N/A"
                audit_date = row.get("LAST_AUDIT_DATE", "N/A") or "N/A"
                report += f"| {row['BRANCH_ID']} | {row['BRANCH_NAME']} | {row['TOTAL_TRANSACTIONS']} | {row['TOTAL_AMOUNT']} | {region} | {audit_date} |\n"
        else:
            report += "No output data available\n"
        
        report += "\n"
        
        # Status and details
        status_emoji = "✅" if result.status == "PASS" else "❌"
        report += f"**Status:** {status_emoji} {result.status}\n\n"
        report += f"**Details:** {result.details}\n\n"
        report += "---\n\n"
    
    return report

def main():
    """
    Main test execution function
    """
    spark = None
    try:
        logger.info("Starting PySpark ETL Pipeline Tests")
        spark = get_spark_session()
        
        # Run test scenarios
        test_results = []
        
        # Test Scenario 1: Insert
        result1 = run_test_scenario_1(spark)
        test_results.append(result1)
        
        # Test Scenario 2: Update
        result2 = run_test_scenario_2(spark)
        test_results.append(result2)
        
        # Generate and print markdown report
        markdown_report = generate_markdown_report(test_results)
        print("\n" + "="*80)
        print("TEST EXECUTION COMPLETE")
        print("="*80)
        print(markdown_report)
        
        # Log summary
        passed_tests = sum(1 for result in test_results if result.status == "PASS")
        total_tests = len(test_results)
        logger.info(f"Test execution completed. {passed_tests}/{total_tests} tests passed.")
        
        return markdown_report
        
    except Exception as e:
        logger.error(f"Test execution failed: {e}")
        return f"# Test Report\n\nTest execution failed with error: {str(e)}"
    finally:
        if spark:
            spark.stop()
            logger.info("Test Spark session stopped.")

if __name__ == "__main__":
    main()