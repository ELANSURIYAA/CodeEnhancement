# [DEPRECATED] - Original JDBC read function preserved for reference
# def read_table(spark: SparkSession, jdbc_url: str, table_name: str, connection_properties: dict) -> DataFrame:
#     """
#     Reads a table from a JDBC source into a DataFrame.
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
    [ADDED] - Reads a Delta table from the specified path.
    
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

def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, 
                               branch_df: DataFrame, branch_operational_df: DataFrame) -> DataFrame:
    """
    [MODIFIED] - Enhanced to integrate BRANCH_OPERATIONAL_DETAILS data.
    Creates the BRANCH_SUMMARY_REPORT DataFrame by aggregating transaction data at the branch level
    and incorporating operational details.
    
    :param transaction_df: DataFrame with transaction data.
    :param account_df: DataFrame with account data.
    :param branch_df: DataFrame with branch data.
    :param branch_operational_df: DataFrame with branch operational details.
    :return: A DataFrame containing the enhanced branch summary report.
    """
    logger.info("Creating Enhanced Branch Summary Report DataFrame.")
    
    # [MODIFIED] - Original aggregation logic preserved
    base_summary = transaction_df.join(account_df, "ACCOUNT_ID") \
                                .join(branch_df, "BRANCH_ID") \
                                .groupBy("BRANCH_ID", "BRANCH_NAME") \
                                .agg(
                                    count("*").alias("TOTAL_TRANSACTIONS"),
                                    spark_sum("AMOUNT").alias("TOTAL_AMOUNT")
                                )
    
    # [ADDED] - Integration with BRANCH_OPERATIONAL_DETAILS
    # Join with branch operational details using left join to preserve all branches
    enhanced_summary = base_summary.join(
        branch_operational_df, 
        base_summary.BRANCH_ID == branch_operational_df.BRANCH_ID, 
        "left"
    ).select(
        base_summary.BRANCH_ID,
        base_summary.BRANCH_NAME,
        base_summary.TOTAL_TRANSACTIONS,
        base_summary.TOTAL_AMOUNT,
        # [ADDED] - Conditional population based on IS_ACTIVE = 'Y'
        when(branch_operational_df.IS_ACTIVE == "Y", branch_operational_df.REGION)
        .otherwise(lit(None)).alias("REGION"),
        when(branch_operational_df.IS_ACTIVE == "Y", branch_operational_df.LAST_AUDIT_DATE)
        .otherwise(lit(None)).alias("LAST_AUDIT_DATE")
    )
    
    logger.info("Enhanced Branch Summary Report created with operational details.")
    return enhanced_summary

def write_to_delta_table(df: DataFrame, table_path: str, mode: str = "overwrite"):
    """
    [MODIFIED] - Enhanced to support different write modes and Delta-specific options.
    Writes a DataFrame to a Delta table.
    
    :param df: The DataFrame to write.
    :param table_path: The path to the target Delta table.
    :param mode: Write mode (overwrite, append, etc.)
    """
    logger.info(f"Writing DataFrame to Delta table: {table_path} with mode: {mode}")
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
    [ADDED] - Validates data quality for the given DataFrame.
    
    :param df: DataFrame to validate
    :param table_name: Name of the table for logging
    :return: True if validation passes, False otherwise
    """
    logger.info(f"Validating data quality for {table_name}")
    
    # Check for empty DataFrame
    row_count = df.count()
    if row_count == 0:
        logger.warning(f"{table_name} is empty")
        return False
    
    # Check for null values in key columns
    if table_name == "BRANCH_SUMMARY_REPORT":
        null_branch_ids = df.filter(col("BRANCH_ID").isNull()).count()
        null_branch_names = df.filter(col("BRANCH_NAME").isNull()).count()
        
        if null_branch_ids > 0 or null_branch_names > 0:
            logger.error(f"{table_name} has null values in key columns")
            return False
    
    logger.info(f"Data quality validation passed for {table_name}. Row count: {row_count}")
    return True

def main():
    """
    [MODIFIED] - Enhanced main ETL execution function with new source integration.
    """
    spark = None
    try:
        spark = get_spark_session()

        # [MODIFIED] - Using sample data instead of JDBC connections
        # Original JDBC connection logic preserved as comments for reference
        # jdbc_url = "jdbc:oracle:thin:@your_oracle_host:1521:orcl"
        # connection_properties = {
        #     "user": "your_user",
        #     "password": "your_password",
        #     "driver": "oracle.jdbc.driver.OracleDriver"
        # }

        # [ADDED] - Create sample data for self-contained execution
        customer_df, branch_df, account_df, transaction_df, branch_operational_df = create_sample_data(spark)
        
        # [DEPRECATED] - Original JDBC read operations
        # customer_df = read_table(spark, jdbc_url, "CUSTOMER", connection_properties)
        # account_df = read_table(spark, jdbc_url, "ACCOUNT", connection_properties)
        # transaction_df = read_table(spark, jdbc_url, "TRANSACTION", connection_properties)
        # branch_df = read_table(spark, jdbc_url, "BRANCH", connection_properties)
        
        # [ADDED] - Read new source table
        # branch_operational_df = read_table(spark, jdbc_url, "BRANCH_OPERATIONAL_DETAILS", connection_properties)

        # Create and write AML_CUSTOMER_TRANSACTIONS (unchanged functionality)
        aml_transactions_df = create_aml_customer_transactions(customer_df, account_df, transaction_df)
        
        # [ADDED] - Data quality validation
        if validate_data_quality(aml_transactions_df, "AML_CUSTOMER_TRANSACTIONS"):
            # [MODIFIED] - Using Delta table paths instead of table names
            write_to_delta_table(aml_transactions_df, "/tmp/delta/aml_customer_transactions")

        # [MODIFIED] - Enhanced branch summary report creation with new source
        branch_summary_df = create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_df)
        
        # [ADDED] - Data quality validation for enhanced report
        if validate_data_quality(branch_summary_df, "BRANCH_SUMMARY_REPORT"):
            write_to_delta_table(branch_summary_df, "/tmp/delta/branch_summary_report")
            
            # [ADDED] - Display sample results for verification
            logger.info("Sample Branch Summary Report with Operational Details:")
            branch_summary_df.show(10, truncate=False)

        logger.info("Enhanced ETL job completed successfully with BRANCH_OPERATIONAL_DETAILS integration.")

    except Exception as e:
        logger.error(f"ETL job failed with exception: {e}")
        raise
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped.")

if __name__ == "__main__":
    main()