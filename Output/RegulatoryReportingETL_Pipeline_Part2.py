# Continuation of RegulatoryReportingETL_Pipeline.py

def create_aml_customer_transactions(customer_df: DataFrame, account_df: DataFrame, transaction_df: DataFrame) -> DataFrame:
    """
    Creates the AML_CUSTOMER_TRANSACTIONS DataFrame by joining customer, account, and transaction data.
    """
    logger.info("Creating AML Customer Transactions DataFrame.")
    return customer_df.join(account_df, "CUSTOMER_ID") \
                      .join(transaction_df, "ACCOUNT_ID") \
                      .select(
                          col("CUSTOMER_ID"),
                          col("NAME"),
                          col("ACCOUNT_ID"),
                          col("TRANSACTION_ID"),
                          col("AMOUNT"),
                          col("TRANSACTION_TYPE"),
                          col("TRANSACTION_DATE")
                      )

def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, branch_df: DataFrame, branch_operational_df: DataFrame) -> DataFrame:
    """
    [MODIFIED] - Creates the BRANCH_SUMMARY_REPORT DataFrame by aggregating transaction data at the branch level.
    Enhanced to include BRANCH_OPERATIONAL_DETAILS integration.
    """
    logger.info("Creating Enhanced Branch Summary Report DataFrame.")
    
    # Original aggregation logic (preserved)
    base_summary = transaction_df.join(account_df, "ACCOUNT_ID") \
                                 .join(branch_df, "BRANCH_ID") \
                                 .groupBy("BRANCH_ID", "BRANCH_NAME") \
                                 .agg(
                                     count("*").alias("TOTAL_TRANSACTIONS"),
                                     sum("AMOUNT").alias("TOTAL_AMOUNT")
                                 )
    
    # [ADDED] - Join with BRANCH_OPERATIONAL_DETAILS and populate new columns conditionally
    logger.info("Integrating BRANCH_OPERATIONAL_DETAILS data")
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
                                      col("REGION"),  # [ADDED] - New column
                                      col("LAST_AUDIT_DATE")  # [ADDED] - New column
                                  )
    
    logger.info("Branch Summary Report enhancement completed")
    return enhanced_summary

def write_to_delta_table(df: DataFrame, table_path: str, mode: str = "overwrite"):
    """
    [MODIFIED] - Writes a DataFrame to Delta table with enhanced options.
    """
    logger.info(f"Writing DataFrame to Delta table: {table_path}")
    try:
        df.write.format("delta") \
          .mode(mode) \
          .option("mergeSchema", "true") \
          .save(table_path)
        logger.info(f"Successfully written data to {table_path}")
    except Exception as e:
        logger.error(f"Failed to write to Delta table {table_path}: {e}")
        raise

def validate_data_quality(df: DataFrame, table_name: str) -> bool:
    """
    [ADDED] - Validates data quality of the DataFrame.
    """
    logger.info(f"Validating data quality for {table_name}")
    
    # Check for null values in key columns
    null_count = df.filter(col("BRANCH_ID").isNull()).count()
    if null_count > 0:
        logger.warning(f"Found {null_count} null BRANCH_ID values in {table_name}")
        return False
    
    # Check for negative amounts
    if "TOTAL_AMOUNT" in df.columns:
        negative_count = df.filter(col("TOTAL_AMOUNT") < 0).count()
        if negative_count > 0:
            logger.warning(f"Found {negative_count} negative amounts in {table_name}")
            return False
    
    logger.info(f"Data quality validation passed for {table_name}")
    return True

def main():
    """
    [MODIFIED] - Main ETL execution function with enhanced logic.
    """
    spark = None
    try:
        spark = get_spark_session()
        
        # [ADDED] - Create sample data for self-contained execution
        customer_df, account_df, transaction_df, branch_df, branch_operational_df = create_sample_data(spark)
        
        logger.info("Sample data created successfully")
        
        # Create and validate AML_CUSTOMER_TRANSACTIONS
        aml_transactions_df = create_aml_customer_transactions(customer_df, account_df, transaction_df)
        if validate_data_quality(aml_transactions_df, "AML_CUSTOMER_TRANSACTIONS"):
            write_to_delta_table(aml_transactions_df, "/tmp/delta/aml_customer_transactions")
        
        # [MODIFIED] - Create and validate enhanced BRANCH_SUMMARY_REPORT
        branch_summary_df = create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_df)
        if validate_data_quality(branch_summary_df, "BRANCH_SUMMARY_REPORT"):
            write_to_delta_table(branch_summary_df, "/tmp/delta/branch_summary_report")
        
        logger.info("Enhanced ETL job completed successfully.")
        
        # [ADDED] - Display results for verification
        logger.info("Branch Summary Report Results:")
        branch_summary_df.show()
        
    except Exception as e:
        logger.error(f"ETL job failed with exception: {e}")
        raise
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped.")

if __name__ == "__main__":
    main()