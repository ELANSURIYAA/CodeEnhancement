# Continuation of RegulatoryReportingETL_Test.py

    def create_branch_summary_report(self, transaction_df, account_df, branch_df, branch_operational_df):
        """Create branch summary report with enhanced logic."""
        # Original aggregation logic
        base_summary = transaction_df.join(account_df, "ACCOUNT_ID") \
                                     .join(branch_df, "BRANCH_ID") \
                                     .groupBy("BRANCH_ID", "BRANCH_NAME") \
                                     .agg(
                                         count("*").alias("TOTAL_TRANSACTIONS"),
                                         sum("AMOUNT").alias("TOTAL_AMOUNT")
                                     )
        
        # Join with BRANCH_OPERATIONAL_DETAILS and populate new columns conditionally
        enhanced_summary = base_summary.join(branch_operational_df, "BRANCH_ID", "left") \
                                      .withColumn(
                                          "REGION", 
                                          when(col("IS_ACTIVE") == "Y", col("REGION")).otherwise(lit(None))
                                      ) \
                                      .withColumn(
                                          "LAST_AUDIT_DATE", 
                                          when(col("IS_ACTIVE") == "Y", col("LAST_AUDIT_DATE")).otherwise(lit(None))
                                      ) \
                                      .select(
                                          col("BRANCH_ID"),
                                          col("BRANCH_NAME"),
                                          col("TOTAL_TRANSACTIONS"),
                                          col("TOTAL_AMOUNT"),
                                          col("REGION"),
                                          col("LAST_AUDIT_DATE")
                                      )
        
        return enhanced_summary
    
    def test_scenario_1_insert(self):
        """Test Scenario 1: Insert new records into target table."""
        logger.info("Running Test Scenario 1: Insert")
        
        try:
            # Create test data
            transaction_df, account_df, branch_df, branch_operational_df = self.create_test_data_scenario1()
            
            # Execute ETL logic
            result_df = self.create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_df)
            
            # Collect results for validation
            results = result_df.collect()
            
            # Validation
            expected_branches = {201, 202}
            actual_branches = {row['BRANCH_ID'] for row in results}
            
            # Check if all expected branches are present
            if expected_branches == actual_branches:
                # Check specific values
                branch_201 = next((row for row in results if row['BRANCH_ID'] == 201), None)
                branch_202 = next((row for row in results if row['BRANCH_ID'] == 202), None)
                
                test_passed = (
                    branch_201 is not None and
                    branch_202 is not None and
                    branch_201['REGION'] == 'Northeast' and
                    branch_202['REGION'] == 'Northwest' and
                    branch_201['TOTAL_TRANSACTIONS'] == 2 and
                    branch_202['TOTAL_TRANSACTIONS'] == 1
                )
                
                if test_passed:
                    self.test_results.append({
                        'scenario': 'Scenario 1: Insert',
                        'status': 'PASS',
                        'input_branches': list(expected_branches),
                        'output_branches': list(actual_branches),
                        'details': f"Successfully inserted {len(results)} branch records"
                    })
                    logger.info("Test Scenario 1: PASSED")
                else:
                    self.test_results.append({
                        'scenario': 'Scenario 1: Insert',
                        'status': 'FAIL',
                        'input_branches': list(expected_branches),
                        'output_branches': list(actual_branches),
                        'details': "Data validation failed"
                    })
                    logger.error("Test Scenario 1: FAILED - Data validation failed")
            else:
                self.test_results.append({
                    'scenario': 'Scenario 1: Insert',
                    'status': 'FAIL',
                    'input_branches': list(expected_branches),
                    'output_branches': list(actual_branches),
                    'details': "Branch ID mismatch"
                })
                logger.error("Test Scenario 1: FAILED - Branch ID mismatch")
                
        except Exception as e:
            self.test_results.append({
                'scenario': 'Scenario 1: Insert',
                'status': 'ERROR',
                'input_branches': [],
                'output_branches': [],
                'details': f"Exception: {str(e)}"
            })
            logger.error(f"Test Scenario 1: ERROR - {str(e)}")
    
    def test_scenario_2_update(self):
        """Test Scenario 2: Update existing records in target table."""
        logger.info("Running Test Scenario 2: Update")
        
        try:
            # Create test data for update scenario
            transaction_df, account_df, branch_df, branch_operational_df = self.create_test_data_scenario2()
            
            # Execute ETL logic
            result_df = self.create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_df)
            
            # Collect results for validation
            results = result_df.collect()
            
            # Validation
            expected_branches = {201, 202}
            actual_branches = {row['BRANCH_ID'] for row in results}
            
            # Check if all expected branches are present
            if expected_branches == actual_branches:
                # Check updated values
                branch_201 = next((row for row in results if row['BRANCH_ID'] == 201), None)
                branch_202 = next((row for row in results if row['BRANCH_ID'] == 202), None)
                
                test_passed = (
                    branch_201 is not None and
                    branch_202 is not None and
                    branch_201['BRANCH_NAME'] == 'Updated Downtown Branch' and
                    branch_202['BRANCH_NAME'] == 'Updated Uptown Branch' and
                    branch_201['TOTAL_TRANSACTIONS'] == 1 and
                    branch_202['TOTAL_TRANSACTIONS'] == 1
                )
                
                if test_passed:
                    self.test_results.append({
                        'scenario': 'Scenario 2: Update',
                        'status': 'PASS',
                        'input_branches': list(expected_branches),
                        'output_branches': list(actual_branches),
                        'details': f"Successfully updated {len(results)} branch records"
                    })
                    logger.info("Test Scenario 2: PASSED")
                else:
                    self.test_results.append({
                        'scenario': 'Scenario 2: Update',
                        'status': 'FAIL',
                        'input_branches': list(expected_branches),
                        'output_branches': list(actual_branches),
                        'details': "Update validation failed"
                    })
                    logger.error("Test Scenario 2: FAILED - Update validation failed")
            else:
                self.test_results.append({
                    'scenario': 'Scenario 2: Update',
                    'status': 'FAIL',
                    'input_branches': list(expected_branches),
                    'output_branches': list(actual_branches),
                    'details': "Branch ID mismatch"
                })
                logger.error("Test Scenario 2: FAILED - Branch ID mismatch")
                
        except Exception as e:
            self.test_results.append({
                'scenario': 'Scenario 2: Update',
                'status': 'ERROR',
                'input_branches': [],
                'output_branches': [],
                'details': f"Exception: {str(e)}"
            })
            logger.error(f"Test Scenario 2: ERROR - {str(e)}")
    
    def generate_markdown_report(self):
        """Generate markdown test report."""
        report = "# Test Report\n\n"
        report += "## PySpark ETL Pipeline Test Results\n\n"
        
        for result in self.test_results:
            report += f"### {result['scenario']}\n\n"
            
            report += "**Input:**\n"
            report += "| Branch ID | Description |\n"
            report += "|-----------|-------------|\n"
            for branch_id in result['input_branches']:
                if result['scenario'] == 'Scenario 1: Insert':
                    desc = "New branch record"
                else:
                    desc = "Existing branch record for update"
                report += f"| {branch_id} | {desc} |\n"
            
            report += "\n**Output:**\n"
            report += "| Branch ID | Status |\n"
            report += "|-----------|--------|\n"
            for branch_id in result['output_branches']:
                report += f"| {branch_id} | Processed |\n"
            
            report += f"\n**Status:** {result['status']}\n"
            report += f"**Details:** {result['details']}\n\n"
            report += "---\n\n"
        
        return report
    
    def run_all_tests(self):
        """Run all test scenarios."""
        logger.info("Starting PySpark ETL Pipeline Tests")
        
        self.setup_spark()
        
        try:
            # Run test scenarios
            self.test_scenario_1_insert()
            self.test_scenario_2_update()
            
            # Generate and return report
            report = self.generate_markdown_report()
            logger.info("All tests completed")
            return report
            
        finally:
            self.teardown_spark()

def main():
    """Main function to run tests."""
    test_runner = TestRegulatoryReportingETL()
    report = test_runner.run_all_tests()
    
    print("\n" + "="*50)
    print("TEST EXECUTION COMPLETED")
    print("="*50)
    print(report)
    
    return report

if __name__ == "__main__":
    main()