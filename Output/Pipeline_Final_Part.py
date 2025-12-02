        # Check for negative amounts
        if "TOTAL_AMOUNT" in df.columns:
            negative_count = df.filter(col("TOTAL_AMOUNT") < 0).count()
            if negative_count > 0:
                logger.warning(f"Found {negative_count} negative TOTAL_AMOUNT values in {table_name}")
                return False
        
        logger.info(f"Data quality validation passed for {table_name}")
        return True
        
    except Exception as e:
        logger.error(f"Data quality validation failed for {table_name}: {e}")
        return False

def main():
    """
    [MODIFIED] - Enhanced main ETL execution function with new source integration
    """
    spark = None
    try:
        spark = get_spark_session()
        
        # [ADDED] - Create sample data for self-contained execution
        customer_df, account_df, transaction_df, branch_df, branch_operational_df = create_sample_data(spark)
        
        logger.info("Sample data created successfully")
        
        # Display sample data for verification
        logger.info("Customer data sample:")
        customer_df.show(5)
        
        logger.info("Branch operational details sample:")
        branch_operational_df.show(5)
        
        # Create and validate AML_CUSTOMER_TRANSACTIONS
        aml_transactions_df = create_aml_customer_transactions(customer_df, account_df, transaction_df)
        
        if validate_data_quality(aml_transactions_df, "AML_CUSTOMER_TRANSACTIONS"):
            write_to_delta_table(aml_transactions_df, "AML_CUSTOMER_TRANSACTIONS")
        else:
            logger.error("Data quality validation failed for AML_CUSTOMER_TRANSACTIONS")
            return
        
        # [MODIFIED] - Create and validate enhanced BRANCH_SUMMARY_REPORT with operational details
        branch_summary_df = create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_df)
        
        logger.info("Enhanced Branch Summary Report with operational details:")
        branch_summary_df.show()
        
        if validate_data_quality(branch_summary_df, "BRANCH_SUMMARY_REPORT"):
            write_to_delta_table(branch_summary_df, "BRANCH_SUMMARY_REPORT")
        else:
            logger.error("Data quality validation failed for BRANCH_SUMMARY_REPORT")
            return
        
        logger.info("ETL job completed successfully with enhanced functionality.")
        
    except Exception as e:
        logger.error(f"ETL job failed with exception: {e}")
        raise
    finally:
        if spark:
            # [MODIFIED] - Removed spark.stop() for Databricks compatibility
            logger.info("ETL execution completed.")

if __name__ == "__main__":
    main()