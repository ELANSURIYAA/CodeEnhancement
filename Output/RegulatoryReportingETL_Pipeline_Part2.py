# Continuation of RegulatoryReportingETL_Pipeline.py

# [DEPRECATED] - Original read_table function for JDBC sources
# def read_table(spark: SparkSession, jdbc_url: str, table_name: str, connection_properties: dict) -> DataFrame:
#     """
#     Reads a table from a JDBC source into a DataFrame.
#     This function is deprecated in favor of Delta table operations.
#     """
#     logger.info(f"Reading table: {table_name}")
#     try:
#         df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
#         return df
#     except Exception as e:
#         logger.error(f"Failed to read table {table_name}: {e}")
#         raise

def create_aml_customer_transactions(customer_df: DataFrame, account_df: DataFrame, transaction_df: DataFrame) -> DataFrame:
    """
    Creates the AML_CUSTOMER_TRANSACTIONS DataFrame by joining customer, account, and transaction data.
    
    :param customer_df: DataFrame with customer data.
    :param account_df: DataFrame with account data.
    :param transaction_df: DataFrame with transaction data.
    :return: A DataFrame ready for the AML customer transactions report.
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

def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, 
                                branch_df: DataFrame, branch_operational_df: DataFrame) -> DataFrame:
    """
    [MODIFIED] - Enhanced to integrate BRANCH_OPERATIONAL_DETAILS table
    Creates the BRANCH_SUMMARY_REPORT DataFrame by aggregating transaction data at the branch level
    and including operational details from the new source table.
    
    :param transaction_df: DataFrame with transaction data.
    :param account_df: DataFrame with account data.
    :param branch_df: DataFrame with branch data.
    :param branch_operational_df: DataFrame with branch operational details (new source).
    :return: A DataFrame containing the enhanced branch summary report.
    """
    logger.info("Creating Enhanced Branch Summary Report DataFrame with operational details.")
    
    # Original aggregation logic (preserved)
    base_summary = transaction_df.join(account_df, "ACCOUNT_ID") \
                                 .join(branch_df, "BRANCH_ID") \
                                 .groupBy("BRANCH_ID", "BRANCH_NAME") \
                                 .agg(
                                     count("*").alias("TOTAL_TRANSACTIONS"),
                                     spark_sum("AMOUNT").alias("TOTAL_AMOUNT")
                                 )
    
    # [ADDED] - Join with BRANCH_OPERATIONAL_DETAILS and add conditional logic
    enhanced_summary = base_summary.join(branch_operational_df, "BRANCH_ID", "left") \
                                   .select(
                                       col("BRANCH_ID"),
                                       col("BRANCH_NAME"),
                                       col("TOTAL_TRANSACTIONS"),
                                       col("TOTAL_AMOUNT"),
                                       # [ADDED] - Conditional population of REGION based on IS_ACTIVE = 'Y'
                                       when(col("IS_ACTIVE") == "Y", col("REGION"))
                                       .otherwise(lit(None)).alias("REGION"),
                                       # [ADDED] - Conditional population of LAST_AUDIT_DATE based on IS_ACTIVE = 'Y'
                                       when(col("IS_ACTIVE") == "Y", col("LAST_AUDIT_DATE"))
                                       .otherwise(lit(None)).alias("LAST_AUDIT_DATE")
                                   )
    
    return enhanced_summary

def write_to_delta_table(df: DataFrame, table_name: str, mode: str = "overwrite"):
    """
    [MODIFIED] - Enhanced to support different write modes
    Writes a DataFrame to a Delta table.
    
    :param df: The DataFrame to write.
    :param table_name: The name of the target Delta table.
    :param mode: Write mode (overwrite, append, merge)
    """
    logger.info(f"Writing DataFrame to Delta table: {table_name} with mode: {mode}")
    try:
        df.write.format("delta") \
          .mode(mode) \
          .option("mergeSchema", "true") \
          .saveAsTable(table_name)
        logger.info(f"Successfully written data to {table_name}")
    except Exception as e:
        logger.error(f"Failed to write to Delta table {table_name}: {e}")
        raise

def validate_data_quality(df: DataFrame, table_name: str) -> bool:
    """
    [ADDED] - Data quality validation function
    Performs basic data quality checks on the DataFrame.
    
    :param df: DataFrame to validate
    :param table_name: Name of the table for logging
    :return: Boolean indicating if validation passed
    """
    logger.info(f"Performing data quality validation for {table_name}")
    
    try:
        # Check for null values in key columns
        null_count = df.filter(col("BRANCH_ID").isNull()).count()
        if null_count > 0:
            logger.warning(f"Found {null_count} null BRANCH_ID values in {table_name}")
            return False
        
        # Check for duplicate BRANCH_IDs
        total_count = df.count()
        distinct_count = df.select("BRANCH_ID").distinct().count()
        if total_count != distinct_count:
            logger.warning(f"Found duplicate BRANCH_ID values in {table_name}")
            return False
        
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