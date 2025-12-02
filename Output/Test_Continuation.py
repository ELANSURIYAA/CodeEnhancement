                'details': f"Branches inserted: {insert_success}, Regions populated: {regions_populated}, Audit dates populated: {audit_dates_populated}"
            })
            
            logger.info(f"Scenario 1 result: {'PASS' if test_passed else 'FAIL'}")
            return test_passed
            
        except Exception as e:
            logger.error(f"Scenario 1 failed with exception: {e}")
            self.test_results.append({
                'scenario': 'Scenario 1: Insert',
                'input_data': 'New branches: [201, 202]',
                'output_data': 'Error occurred',
                'status': 'FAIL',
                'details': f"Exception: {str(e)}"
            })
            return False
    
    def test_scenario_2_update(self):
        """
        Test Scenario 2: Update existing rows in target table
        """
        logger.info("Running Test Scenario 2: Update")
        
        try:
            # Create test data
            branch_df, account_df, transaction_df, branch_operational_df = self.create_test_data_scenario_2()
            
            # Create branch summary report
            result_df = self.create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_df)
            
            # Collect results for validation
            results = result_df.collect()
            
            # Validate results
            expected_branch_ids = [101, 102]
            actual_branch_ids = [row['BRANCH_ID'] for row in results]
            
            # Check if expected branches are present
            update_success = all(branch_id in actual_branch_ids for branch_id in expected_branch_ids)
            
            # Check if totals are updated (should be higher than original)
            # Branch 101 should have more transactions and higher amount
            branch_101_data = next((row for row in results if row['BRANCH_ID'] == 101), None)
            branch_102_data = next((row for row in results if row['BRANCH_ID'] == 102), None)
            
            totals_updated = (
                branch_101_data and branch_101_data['TOTAL_TRANSACTIONS'] >= 2 and
                branch_102_data and branch_102_data['TOTAL_TRANSACTIONS'] >= 2
            )
            
            # Check if regions and audit dates are updated
            regions_updated = all(row['REGION'] is not None for row in results)
            audit_dates_updated = all(row['LAST_AUDIT_DATE'] is not None for row in results)
            
            test_passed = update_success and totals_updated and regions_updated and audit_dates_updated
            
            self.test_results.append({
                'scenario': 'Scenario 2: Update',
                'input_data': f"Updated branches: {expected_branch_ids}",
                'output_data': results,
                'status': 'PASS' if test_passed else 'FAIL',
                'details': f"Branches updated: {update_success}, Totals updated: {totals_updated}, Regions updated: {regions_updated}, Audit dates updated: {audit_dates_updated}"
            })
            
            logger.info(f"Scenario 2 result: {'PASS' if test_passed else 'FAIL'}")
            return test_passed
            
        except Exception as e:
            logger.error(f"Scenario 2 failed with exception: {e}")
            self.test_results.append({
                'scenario': 'Scenario 2: Update',
                'input_data': 'Updated branches: [101, 102]',
                'output_data': 'Error occurred',
                'status': 'FAIL',
                'details': f"Exception: {str(e)}"
            })
            return False
    
    def generate_markdown_report(self) -> str:
        """
        Generate markdown test report
        """
        report = "# Test Report\n\n"
        
        for result in self.test_results:
            report += f"## {result['scenario']}\n\n"
            report += f"**Input:** {result['input_data']}\n\n"
            
            if isinstance(result['output_data'], list) and len(result['output_data']) > 0:
                report += "**Output:**\n\n"
                report += "| BRANCH_ID | BRANCH_NAME | TOTAL_TRANSACTIONS | TOTAL_AMOUNT | REGION | LAST_AUDIT_DATE |\n"
                report += "|-----------|-------------|-------------------|--------------|--------|-----------------|\n"
                
                for row in result['output_data']:
                    report += f"| {row['BRANCH_ID']} | {row['BRANCH_NAME']} | {row['TOTAL_TRANSACTIONS']} | {row['TOTAL_AMOUNT']} | {row['REGION']} | {row['LAST_AUDIT_DATE']} |\n"
            else:
                report += f"**Output:** {result['output_data']}\n\n"
            
            report += f"\n**Status:** {result['status']}\n\n"
            report += f"**Details:** {result['details']}\n\n"
            report += "---\n\n"
        
        return report
    
    def run_all_tests(self):
        """
        Run all test scenarios and generate report
        """
        logger.info("Starting ETL Test Suite")
        
        # Run test scenarios
        scenario_1_passed = self.test_scenario_1_insert()
        scenario_2_passed = self.test_scenario_2_update()
        
        # Generate and print report
        report = self.generate_markdown_report()
        print("\n" + "="*80)
        print("ETL TEST RESULTS")
        print("="*80)
        print(report)
        
        # Summary
        total_tests = len(self.test_results)
        passed_tests = sum(1 for result in self.test_results if result['status'] == 'PASS')
        
        print(f"\n## Summary")
        print(f"Total Tests: {total_tests}")
        print(f"Passed: {passed_tests}")
        print(f"Failed: {total_tests - passed_tests}")
        print(f"Success Rate: {(passed_tests/total_tests)*100:.1f}%")
        
        return report

def main():
    """
    Main function to run the ETL tests
    """
    try:
        tester = ETLTester()
        report = tester.run_all_tests()
        return report
    except Exception as e:
        logger.error(f"Test execution failed: {e}")
        return f"Test execution failed: {e}"

if __name__ == "__main__":
    main()