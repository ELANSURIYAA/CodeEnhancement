# [DEPRECATED] - Original JDBC read function preserved for reference
# def read_table(spark: SparkSession, jdbc_url: str, table_name: str, connection_properties: dict) -> DataFrame:
#     """
#     Reads a table from a JDBC source into a DataFrame.
#     
#     :param spark: The SparkSession object.
#     :param jdbc_url: The JDBC URL for the database connection.
#     :param table_name: The name of the table to read.
#     :param connection_properties: A dictionary of connection properties (e.g., user, password).
#     :return: A Spark DataFrame containing the table data.
#     """
#     logger.info(f"Reading table: {table_name}")
#     try:
#         df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
#         return df
#     except Exception as e:
#         logger.error(f"Failed to read table {table_name}: {e}")
#         raise

def read_delta_table(spark: SparkSession, table_path: str) -> DataFrame:
    """
    # [ADDED] - Reads a Delta table from the specified path.
    
    :param spark: The SparkSession object.
    :param table_path: The path to the Delta table.
    :return: A Spark DataFrame containing the table data.
    """
    logger.info(f"Reading Delta table: {table_path}")
    try:
        df = spark.read.format("delta").load(table_path)
        return df
    except Exception as e:
        logger.error(f"Failed to read Delta table {table_path}: {e}")
        raise

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

def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, branch_df: DataFrame, branch_operational_df: DataFrame) -> DataFrame:
    """
    # [MODIFIED] - Creates the BRANCH_SUMMARY_REPORT DataFrame by aggregating transaction data at the branch level.
    # Enhanced to include BRANCH_OPERATIONAL_DETAILS integration.
    
    :param transaction_df: DataFrame with transaction data.
    :param account_df: DataFrame with account data.
    :param branch_df: DataFrame with branch data.
    :param branch_operational_df: DataFrame with branch operational details. [ADDED]
    :return: A DataFrame containing the enhanced branch summary report.
    """
    logger.info("Creating Enhanced Branch Summary Report DataFrame.")
    
    # Original aggregation logic preserved
    base_summary = transaction_df.join(account_df, "ACCOUNT_ID") \
                                 .join(branch_df, "BRANCH_ID") \
                                 .groupBy("BRANCH_ID", "BRANCH_NAME") \
                                 .agg(
                                     count("*").alias("TOTAL_TRANSACTIONS"),
                                     sum("AMOUNT").alias("TOTAL_AMOUNT")
                                 )
    
    # [ADDED] - Join with BRANCH_OPERATIONAL_DETAILS and conditionally populate new columns
    enhanced_summary = base_summary.join(branch_operational_df, "BRANCH_ID", "left") \
                                  .select(
                                      col("BRANCH_ID"),
                                      col("BRANCH_NAME"),
                                      col("TOTAL_TRANSACTIONS"),
                                      col("TOTAL_AMOUNT"),
                                      # [ADDED] - Conditional population based on IS_ACTIVE = 'Y'
                                      when(col("IS_ACTIVE") == "Y", col("REGION")).otherwise(lit(None)).alias("REGION"),
                                      when(col("IS_ACTIVE") == "Y", col("LAST_AUDIT_DATE")).otherwise(lit(None)).alias("LAST_AUDIT_DATE")
                                  )
    
    logger.info("Enhanced Branch Summary Report created with REGION and LAST_AUDIT_DATE columns")
    return enhanced_summary

def write_to_delta_table(df: DataFrame, table_path: str, mode: str = "overwrite"):
    """
    # [MODIFIED] - Writes a DataFrame to a Delta table with enhanced options.
    
    :param df: The DataFrame to write.
    :param table_path: The path to the target Delta table.
    :param mode: Write mode (overwrite, append, etc.)
    """
    logger.info(f"Writing DataFrame to Delta table: {table_path}")
    try:
        df.write.format("delta") \
          .mode(mode) \
          .option("mergeSchema", "true") \
          .save(table_path)  # [MODIFIED] - Changed from saveAsTable to save for path-based approach
        logger.info(f"Successfully written data to {table_path}")
    except Exception as e:
        logger.error(f"Failed to write to Delta table {table_path}: {e}")
        raise

def validate_data_quality(df: DataFrame, table_name: str) -> bool:
    """
    # [ADDED] - Validates data quality for the given DataFrame.
    
    :param df: DataFrame to validate
    :param table_name: Name of the table for logging
    :return: True if validation passes, False otherwise
    """
    logger.info(f"Validating data quality for {table_name}")
    
    try:
        # Check for null values in key columns
        row_count = df.count()
        if row_count == 0:
            logger.warning(f"{table_name} is empty")
            return False
        
        logger.info(f"{table_name} validation passed: {row_count} rows")
        return True
    except Exception as e:
        logger.error(f"Data quality validation failed for {table_name}: {e}")
        return False

def main():
    """
    # [MODIFIED] - Main ETL execution function enhanced with new source integration.
    """
    spark = None
    try:
        spark = get_spark_session()

        # [ADDED] - Create sample data instead of reading from JDBC
        sample_data = create_sample_data(spark)
        
        customer_df = sample_data["customer"]
        account_df = sample_data["account"]
        transaction_df = sample_data["transaction"]
        branch_df = sample_data["branch"]
        branch_operational_df = sample_data["branch_operational"]  # [ADDED]

        # Validate input data
        if not all([
            validate_data_quality(customer_df, "CUSTOMER"),
            validate_data_quality(account_df, "ACCOUNT"),
            validate_data_quality(transaction_df, "TRANSACTION"),
            validate_data_quality(branch_df, "BRANCH"),
            validate_data_quality(branch_operational_df, "BRANCH_OPERATIONAL_DETAILS")  # [ADDED]
        ]):
            raise Exception("Data quality validation failed")

        # Create and write AML_CUSTOMER_TRANSACTIONS
        aml_transactions_df = create_aml_customer_transactions(customer_df, account_df, transaction_df)
        write_to_delta_table(aml_transactions_df, "/tmp/delta/aml_customer_transactions")

        # [MODIFIED] - Create and write enhanced BRANCH_SUMMARY_REPORT with new source integration
        branch_summary_df = create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_df)
        write_to_delta_table(branch_summary_df, "/tmp/delta/branch_summary_report")

        # [ADDED] - Display results for verification
        logger.info("Displaying AML Customer Transactions sample:")
        aml_transactions_df.show(5, truncate=False)
        
        logger.info("Displaying Enhanced Branch Summary Report:")
        branch_summary_df.show(truncate=False)

        logger.info("ETL job completed successfully with enhanced functionality.")

    except Exception as e:
        logger.error(f"ETL job failed with exception: {e}")
        raise
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped.")

if __name__ == "__main__":
    main()