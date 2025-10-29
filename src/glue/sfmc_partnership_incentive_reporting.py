import sys
from datetime import datetime

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import coalesce
from pyspark.sql.functions import col
from pyspark.sql.functions import date_format
from pyspark.sql.functions import lit
from pyspark.sql.functions import to_date
from pyspark.sql.functions import udf
from pyspark.sql.types import DateType
from pyspark.sql.types import FloatType
from pyspark.sql.window import Window

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()


customer_sort_list = ["dynamodb_new_image_updated_at_n"]
customer_group_list = ["customer_id"]
client_sort_list = ["last_modified_date"]
client_group_list = ["encoded_key", "id"]
account_sort_list = ["encoded_key", "account_holder_key", "name", "last_modified_date"]
account_group_list = ["encoded_key", "account_holder_key", "name"]
transaction_sort_list = [
    "parent_account_key",
    "id_deposit_transactions",
    "booking_date",
]
transaction_group_list = [
    "parent_account_key",
    "id_deposit_transactions",
    "type",
    "transaction_details_transaction_channel_id",
]

customer_transaction_columns = [
    "customer_id",
    "dynamodb_new_image_brand_id_s",
    "account_holder_key",
    "name",
    "parent_account_key",
    "id_deposit_transactions",
    "type",
    "transaction_details_transaction_channel_id",
    "currency_code_deposit_transaction",
    "amount",
    "affected_amounts_funds_amount",
    "balances_total_balance",
    "approved_date",
    "booking_date",
]

channel_filter_list = [
    "BLME_USD_Deposit",
    "CB_Deposit_BACS",
    "CB_Deposit_CHAPS",
    "CB_Deposit_FPS",
    "CB_Transfer",
    "CC_Deposit_AED",
    "CC_Deposit_EUR",
    "CC_Deposit_GBP",
    "CC_Deposit_KWD",
    "CC_Deposit_SAR",
    "CC_Deposit_USD",
    "CC_ManualDeposit_AED",
    "CC_ManualDeposit_EUR",
    "CC_ManualDeposit_GBP",
    "CC_ManualDeposit_KWD",
    "CC_ManualDeposit_SAR",
    "CC_ManualDeposit_USD",
    "ClearBank_Nostro_GBP",
]

inbound_cols = [
    "total_inbound_payment_gbp",
    "total_inbound_payment_sar",
    "total_inbound_payment_aed",
    "total_inbound_payment_usd",
    "total_inbound_payment_eur",
    "total_inbound_payment_kwd",
]

ftd_balance_cols = [
    "gbp_ftd_balance",
    "gbp_standard_ftd_balance",
    "gbp_exclusive_ftd_balance",
    "eur_ftd_balance",
    "eur_standard_ftd_balance",
    "eur_exclusive_ftd_balance",
    "usd_ftd_balance",
    "usd_standard_ftd_balance",
    "usd_exclusive_ftd_balance",
]

balance_cols = [
    "last_balance_gbp",
    "last_balance_sar",
    "last_balance_aed",
    "last_balance_usd",
    "last_balance_eur",
    "last_balance_kwd",
]

maturity_cols = [
    "gbp_ftd_maturity_date",
    "gbp_standard_ftd_maturity_date",
    "gbp_exclusive_ftd_maturity_date",
    "eur_ftd_maturity_date",
    "eur_standard_ftd_maturity_date",
    "eur_exclusive_ftd_maturity_date",
    "usd_ftd_maturity_date",
    "usd_standard_ftd_maturity_date",
    "usd_exclusive_ftd_maturity_date",
]

ftd_account_opening_cols = [
    "gbp_ftd_last_in_date",
    "gbp_standard_ftd_last_in_date",
    "gbp_exclusive_ftd_last_in_date",
    "eur_ftd_last_in_date",
    "eur_standard_ftd_last_in_date",
    "eur_exclusive_ftd_last_in_date",
    "usd_ftd_last_in_date",
    "usd_standard_ftd_last_in_date",
    "usd_exclusive_ftd_last_in_date",
]

date_cols = [
    "gbp_ftd_maturity_date",
    "eur_ftd_last_in_date",
    "gbp_standard_ftd_maturity_date",
    "gbp_exclusive_ftd_maturity_date",
    "eur_ftd_maturity_date",
    "eur_standard_ftd_maturity_date",
    "eur_standard_ftd_last_in_date",
    "eur_exclusive_ftd_maturity_date",
    "eur_exclusive_ftd_last_in_date",
    "usd_ftd_maturity_date",
    "usd_standard_ftd_maturity_date",
    "usd_exclusive_ftd_maturity_date",
    "last_transaction_date",
    "approved_date",
    "first_deposit_transaction_date",
    "submit_application_datetime",
]

final_cols = [
    "customer_id",
    "total_inbound_payment_gbp",
    "last_balance_gbp",
    "gbp_ftd_maturity_date",
    "gbp_standard_ftd_maturity_date",
    "gbp_exclusive_ftd_maturity_date",
    "total_inbound_payment_eur",
    "last_balance_eur",
    "eur_ftd_maturity_date",
    "eur_ftd_last_in_date",
    "eur_standard_ftd_balance",
    "eur_standard_ftd_maturity_date",
    "eur_standard_ftd_last_in_date",
    "eur_exclusive_ftd_balance",
    "eur_exclusive_ftd_maturity_date",
    "eur_exclusive_ftd_last_in_date",
    "total_inbound_payment_usd",
    "last_balance_usd",
    "usd_ftd_maturity_date",
    "usd_standard_ftd_maturity_date",
    "usd_exclusive_ftd_maturity_date",
    "total_inbound_payment_aed",
    "last_balance_aed",
    "total_inbound_payment_sar",
    "last_balance_sar",
    "total_inbound_payment_kwd",
    "last_balance_kwd",
    "last_transaction_date",
    "approved_date",
    "first_deposit_transaction_date",
    "submit_application_datetime",
]


customers_sql = """
SELECT DISTINCT dynamodb_keys_id_s, dynamodb_new_image_id_s, dynamodb_new_image_brand_id_s,
dynamodb_old_image_brand_id_s, dynamodb_new_image_status_s, dynamodb_newimage_individual_m_address_m_provider_s,
dynamodb_new_image_updated_at_n
FROM datalake_raw.dynamo_scv_sls_customers
"""
clients_sql = (
    """SELECT DISTINCT id, encoded_key, last_modified_date FROM datalake_raw.clients"""
)

deposit_accounts_sql = """
SELECT DISTINCT encoded_key, account_holder_key, name, currency_code, balances_total_balance,
maturity_date, approved_date, activation_date, last_modified_date
FROM datalake_raw.deposit_accounts
"""
deposit_transactions_sql = """SELECT DISTINCT parent_account_key, id, booking_date, creation_date, type, currency_code,
                              amount, affected_amounts_funds_amount,
                              transaction_details_transaction_channel_id
                              FROM datalake_raw.deposit_transactions"""


def read_athena(sql: str, input_database: str, **kwargs) -> "DataFrame":
    """
    Read data from Athena via Spark SQL.
    """
    if kwargs:
        formatted_sql = sql % tuple(kwargs.values())
    else:
        formatted_sql = sql
    # Assuming the database is already registered as a Spark catalog database
    logger.info(f"Reading from Athena with query: {formatted_sql}")
    # Read data into a Spark DataFrame
    df = spark.sql(formatted_sql)
    logger.info("Read SQL query success.")
    return df


def get_customer_data(sql, database, customer_id1=None, customer_id2=None, **kwargs):
    """
    Read and convert customer and client data into a DataFrame.
    Populate any missing customer IDs.
    Parameters:
    sql: str - SQL query
    database: str - name of the database
    customer_id1: str - name of a customer ID column
    customer_id2: str - name of another customer ID column
    kwargs: dynamic input variables for SQL query
    """
    logger.info("get_customer_data start.")
    df = read_athena(sql, database, **kwargs)
    # customer_count = df.select(customer_id1).distinct().count()
    # logger.info(f"Number of customers returned from SQL query: {customer_count}")
    df = convert_strings_to_dates(df)
    if customer_id1 in df.columns:
        df = df.withColumn(
            "customer_id", coalesce(col(customer_id1), col(customer_id2))
        )
    logger.info("get_customer_data End.")
    return df


def get_client_data(sql, database, **kwargs):
    """
    Read and convert customer and client data into a DataFrame.
    Populate any missing customer IDs.
    Parameters:
    sql: str - SQL query
    database: str - name of the database
    customer_id1: str - name of a customer ID column
    customer_id2: str - name of another customer ID column
    kwargs: dynamic input variables for SQL query
    """
    logger.info("get_client_data start.")
    df = read_athena(sql, database, **kwargs)
    df = convert_strings_to_dates(df)
    logger.info("get_client_data End.")
    return df


def convert_strings_to_dates(df):
    """
    Convert DataFrame columns from string to datetime.
    Parameters:
    df: pyspark.sql.DataFrame - input DataFrame
    """
    # Identify columns that have "date" in their name
    selected_cols = [col_name for col_name in df.columns if "date" in col_name.lower()]
    for s_col in selected_cols:
        df = df.withColumn(s_col, date_format(col(s_col), "yyyy-MM-dd HH:mm:ss.SSS"))
        # df = df.withColumn(s_col, col(s_col).cast("timestamp"))
    logger.info(f"Converted the following columns to date formats: {selected_cols}")
    return df


def convert_strings_to_numbers(df):
    """
    Convert string columns containing 'balance' or 'amount' to float
    """
    balance_columns = [
        col_name
        for col_name in df.columns
        if "balance" in col_name or "amount" in col_name
    ]
    for col_name in balance_columns:
        df = df.withColumn(col_name, col(col_name).cast(FloatType()))
    logger.info(f"Converted the following to numeric formats: {balance_columns}")
    return df


def get_deposit_data(sql, database, **kwargs):
    """
    Read and process deposit data from Athena using Spark
    """
    # Read data from Athena using Spark SQL
    df = read_athena(sql, database, **kwargs)
    logger.info("Read Athena data Done.")
    # Convert string columns with 'date' in their name to datetime
    df = convert_strings_to_dates(df)
    logger.info("Convert strings to dates Done.")
    # Convert string columns containing 'balance' or 'amount' to float
    df = convert_strings_to_numbers(df)
    logger.info("Convert strings to numbers Done.")
    return df


# Function to merge client IDs with customer dataframe
def customer_to_client(df_c, df_cl):
    """
    Merge client ids to the customer dataframe.
    """
    logger.info("Customer to client start.")
    df_ccl = df_c.join(df_cl, df_c.customer_id == df_cl.id, how="left").select(
        df_c["*"],
        df_cl.encoded_key.alias("encoded_key_clients"),
        df_cl.last_modified_date.alias("last_modified_date_clients"),
        df_cl.id.alias("id_clients"),
    )
    logger.info("Customer to client end.")
    return df_ccl


# Function to merge customer, client, and deposit account data


def customer_to_deposit_accounts(df_ccl, df_da):
    """
    Merge customer and client data with the deposit dataframe.
    """
    logger.info("Customer to deposit accounts start.")
    df_da = df_da.select(
        df_da.encoded_key.alias("encoded_key_deposit_accounts"),
        df_da.last_modified_date.alias("last_modified_date_deposit_accounts"),
        df_da.currency_code.alias("currency_code_deposit_accounts"),
        "account_holder_key",
        "name",
        "approved_date",
        "balances_total_balance",
    )
    df_cclda = df_ccl.join(
        df_da, df_ccl.encoded_key_clients == df_da.account_holder_key, how="left"
    )
    selected_columns = [
        "customer_id",
        "dynamodb_new_image_brand_id_s",
        "id_clients",
        "encoded_key_clients",
        "encoded_key_deposit_accounts",
        "account_holder_key",
        "name",
        "approved_date",
        "balances_total_balance",
        "last_modified_date_deposit_accounts",
    ]
    df_cclda = df_cclda.select(selected_columns)
    logger.info("Customer to deposit accounts end.")
    return df_cclda


# Function to transform deposit transactions


def deposit_transaction_transform(df_deposit_transactions):
    """
    Convert amounts and dates to the correct format.
    """
    logger.info("Deposit transaction transform start.")
    df_deposit_transactions = convert_strings_to_numbers(df_deposit_transactions)
    df_deposit_transactions = convert_strings_to_dates(df_deposit_transactions)
    df_dt = df_deposit_transactions.withColumnRenamed(
        "id", "id_deposit_transactions"
    ).withColumnRenamed("currency_code", "currency_code_deposit_transaction")
    logger.info("Deposit transaction transform end.")
    return df_dt


# Function to join customer and deposit account data with transaction data


def customers_accounts_transactions(df_cclda, df_dt, customer_transaction_columns):
    """
    Join customer data to transaction data.
    """
    df = df_cclda.join(
        df_dt,
        df_cclda.encoded_key_deposit_accounts == df_dt.parent_account_key,
        how="inner",
    )
    df = df.select(customer_transaction_columns)
    logger.info("Customers accounts transactions done.")
    return df


def merge_customer_and_deposit_data(*args):
    df_customers = args[0]
    df_clients = args[1]
    df_deposit_accounts = args[2]
    df_deposit_transactions = args[3]
    customer_sort_list = args[4]
    client_sort_list = args[5]
    account_sort_list = args[6]
    transaction_sort_list = args[7]
    customer_group_list = args[8]
    client_group_list = args[9]
    account_group_list = args[10]
    transaction_group_list = args[11]
    customer_transaction_columns = args[12]
    # Filter unique records for customers, clients, accounts, and transactions
    df_c = filter_unique_records(df_customers, customer_sort_list, customer_group_list)
    df_cl = filter_unique_records(df_clients, client_sort_list, client_group_list)
    df_da = filter_unique_records(
        df_deposit_accounts, account_sort_list, account_group_list
    )
    # Merge customer to client data
    df_ccl = customer_to_client(df_c, df_cl)
    # Merge customer and client data with deposit account data
    df_cclda = customer_to_deposit_accounts(df_ccl, df_da)
    # Transform deposit transactions
    df_dt = deposit_transaction_transform(df_deposit_transactions)
    # Filter unique deposit transactions
    df_dt1 = filter_unique_records_2(
        df_dt, transaction_sort_list, transaction_group_list
    )
    # Merge customers, accounts, and transactions
    df = customers_accounts_transactions(df_cclda, df_dt1, customer_transaction_columns)
    logger.info("Merge customer and deposit data done.")
    return df


def filter_unique_records(df, sort_list, groupby_list):
    """
    Filter & sort PySpark DataFrame for specific and unique records
    based on input variables. Handles empty or null values by giving them the least priority.
    Parameter 1: df: DataFrame - Data to be processed
    Parameter 2: sort_list: List - list of columns to sort on
    Parameter 3: groupby_list: List - list of columns to group on
    """
    # Define the name for the new row number column
    row_name_col = "df_row_number"
    selected_cols = [col_name for col_name in df.columns if "date" in col_name.lower()]
    for col in selected_cols:
        earliest_date = "1900-01-01 00:00:00"  # Define earliest possible date
        df = df.withColumn(
            col,
            F.when(
                F.col(col).isNull() | (F.col(col) == ""), F.lit(earliest_date)
            ).otherwise(F.col(col)),
        )
    # Step 1: Define a Window specification to partition by `groupby_list`
    # and order within each partition by `sort_list` in descending order
    window_spec = Window.partitionBy(groupby_list).orderBy(
        [F.col(col).desc() for col in sort_list]
    )
    # Step 2: Create a new column with row number (equivalent to cumcount() + 1 in Pandas)
    df = df.withColumn(row_name_col, F.row_number().over(window_spec))
    # Step 3: Filter the rows where the row number is 1 (equivalent to df[df[row_name_col] == 1])
    df_unique = df.filter(F.col(row_name_col) == 1)
    # Step 4: Drop the `row_name_col` from the DataFrame
    df_unique = df_unique.drop(row_name_col)
    logger.info("Filter unique records done.")
    return df_unique


def filter_unique_records_2(df, sort_list, groupby_list):
    """
    Filter & sort PySpark DataFrame for specific and unique records
    based on input variables. Handles empty or null values by giving them the least priority.
    Parameter 1: df: DataFrame - Data to be processed
    Parameter 2: sort_list: List - list of columns to sort on
    Parameter 3: groupby_list: List - list of columns to group on
    """
    # Define the name for the new row number column
    row_name_col = "df_row_number"
    # Step 0: Filter out rows where "transaction_details_transaction_channel_id" is null or empty
    df = df.filter(F.col("transaction_details_transaction_channel_id").isNotNull())
    # Handle empty strings (if necessary) in "transaction_details_transaction_channel_id"
    df = df.filter(F.col("transaction_details_transaction_channel_id") != "")
    selected_cols = [col_name for col_name in df.columns if "date" in col_name.lower()]
    for col in selected_cols:
        earliest_date = "1900-01-01 00:00:00"  # Define earliest possible date
        df = df.withColumn(
            col,
            F.when(
                F.col(col).isNull() | (F.col(col) == ""), F.lit(earliest_date)
            ).otherwise(F.col(col)),
        )
    # Step 1: Define a Window specification to partition by `groupby_list`
    # and order within each partition by `sort_list` in descending order
    window_spec = Window.partitionBy(groupby_list).orderBy(
        [F.col(col).desc() for col in sort_list]
    )
    # Step 2: Create a new column with row number (equivalent to cumcount() + 1 in Pandas)
    df2 = df.withColumn(row_name_col, F.row_number().over(window_spec))
    # Step 3: Filter the rows where the row number is 1 (equivalent to df[df[row_name_col] == 1])
    df_unique = df2.filter(F.col(row_name_col) == 1)
    # Step 4: Drop the `row_name_col` from the DataFrame
    df_unique = df_unique.drop(row_name_col)
    logger.info("Filter unique records 2 done.")
    return df_unique


def build_submission_dates(df_start_customers, customer_sort_list, customer_group_list):
    """
    ETL process to build submit application customer dataframe
    """
    # Assuming filter_unique_records is a function that filters the dataframe
    df_sc = filter_unique_records(
        df_start_customers, customer_sort_list, customer_group_list
    )
    df_sc = submit_application_dates(df_sc)
    logger.info("building applicant submit dates...")
    return df_sc


def submit_application_dates(df):
    """
    Add submit application datetime to customer dataframe
    """
    # Renaming the column
    df = df.withColumnRenamed(
        "dynamodb_new_image_updated_at_n", "submit_application_datetime"
    )
    # Selecting the necessary columns
    start_columns = ["customer_id", "submit_application_datetime"]
    df = df.select(*start_columns)
    logger.info("submit application dates done.")
    return df


def first_deposit_transactions(df, filter=None):
    """
    Filter for first deposit transaction dates using PySpark
    Parameter 1: DataFrame: Deposit transactions
    Parameter 2: full list or partial list of channel ids
    """
    # Filter for deposit transactions
    if filter is None:
        deposit_filter = df["type"] == "DEPOSIT"
    else:
        deposit_filter = F.col("transaction_details_transaction_channel_id").rlike(
            "|".join(filter)
        )
    df_filtered = df.filter(deposit_filter)
    df_filtered = df_filtered.withColumn("booking_date", F.to_timestamp("booking_date"))
    # Creating window specification based on parent_account_key
    window_spec = Window.partitionBy("parent_account_key").orderBy("booking_date")
    # Adding a cumulative count for deposits to mark the first deposit booking flag
    df_filtered = df_filtered.withColumn(
        "first_deposit_booking_flag", F.row_number().over(window_spec)
    )
    # Filter for the first deposit transaction
    df_first_deposit = df_filtered.filter(F.col("first_deposit_booking_flag") == 1)
    # Select and rename relevant columns
    df_first_deposit = df_first_deposit.select(
        "customer_id",
        "parent_account_key",
        F.col("booking_date").alias("first_deposit_transaction_date"),
    )
    # Create a window spec based on customer_id to rank transactions
    customer_window = Window.partitionBy("customer_id").orderBy(
        "first_deposit_transaction_date"
    )
    # Adding a ranking column to track the first deposit transaction per customer
    df_first_deposit = df_first_deposit.withColumn(
        "customer_booking_rank", F.row_number().over(customer_window)
    )
    # Filter for the first booking rank
    df_first_dates = df_first_deposit.filter(F.col("customer_booking_rank") == 1)
    # Logging info for tracking (if using a logger)
    logger.info("building first deposit dates")
    return df_first_dates


def last_deposit_transactions(df):
    """
    Filter for the last deposit by date using PySpark
    """
    # Group by customer_id and calculate the maximum booking_date (last deposit)
    df_max_booking = df.groupBy("customer_id").agg(
        F.max("booking_date").alias("last_deposit_transaction_date")
    )
    # Join back with the original DataFrame to retrieve the 'name' column
    df_max_booking_with_name = df_max_booking.join(
        df.select("customer_id", "name").distinct(), on="customer_id", how="left"
    )
    # Drop duplicates based on relevant columns
    df_max_booking_with_name = df_max_booking_with_name.dropDuplicates(
        ["customer_id", "last_deposit_transaction_date"]
    )
    # Select only the required columns: customer_id, name, and last_deposit_transaction_date
    df_max_booking_with_name = df_max_booking_with_name.select(
        "customer_id", "name", "last_deposit_transaction_date"
    )
    # Logging info for tracking (if using a logger)
    logger.info("building last deposit dates")
    return df_max_booking_with_name


def last_debit_transaction(df):
    "Filter for last transaction or withdrawal by date"
    # Filter for rows where type is either WITHDRAWAL or WITHDRAWAL_ADJUSTMENT
    df_last_spending = df.filter(
        (df["type"] == "WITHDRAWAL") | (df["type"] == "WITHDRAWAL_ADJUSTMENT")
    )
    # Group by customer_id and find the maximum booking_date for each customer
    df_last_customer_spending = df_last_spending.groupBy("customer_id").agg(
        F.max("booking_date").alias("last_transaction_date")
    )
    logger.info("last debit transaction done.")
    return df_last_customer_spending


def first_customer_approved_dates(spark_df):
    """
    Filter for first approved date per customer using PySpark in AWS Glue.
    Parameters:
    spark_df: Input PySpark DataFrame with columns 'customer_id' and 'approved_date'
    Returns:
    PySpark DataFrame with the first approved date per customer
    """
    # Convert the 'approved_date' column to a timestamp (equivalent of pd.to_datetime in Pandas)
    spark_df = spark_df.withColumn("approved_date", F.to_timestamp("approved_date"))
    # Group by 'customer_id' and calculate the minimum 'approved_date'
    df_first_customer_approved_dt = spark_df.groupBy("customer_id").agg(
        F.min("approved_date").alias("approved_date")
    )
    logger.info("first customer approved dates.")
    return df_first_customer_approved_dt


def build_inbound_deposit_values(df, inbound_cols, filter=None):
    """ETL process to build inbound payments per customer.
    Can also filter on transaction channel id."""
    df_dt_sum_deposit = deposit_transaction_sum(df, filter)
    df_dt_sum_deposit = apply_transpose(
        df_dt_sum_deposit,
        inbound_cols,
        transpose_currencies_values,
        "amount",
        "currency_code_deposit_transaction",
    )
    df_deposit_ccy_total = deposit_by_currency_totals(df_dt_sum_deposit, inbound_cols)
    logger.info("building inbound deposit values...")
    return df_deposit_ccy_total


def deposit_transaction_sum(df, filter=None):
    """Sum of all deposits per account and per currency"""
    # Select relevant columns
    df_dt_sum = df.select(
        "customer_id",
        "account_holder_key",
        "type",
        "transaction_details_transaction_channel_id",
        "name",
        "currency_code_deposit_transaction",
        "amount",
    )
    # Group by customer, account, and other necessary fields, then sum the amounts
    df_dt_sum = df_dt_sum.groupBy(
        "customer_id",
        "account_holder_key",
        "type",
        "transaction_details_transaction_channel_id",
        "name",
        "currency_code_deposit_transaction",
    ).agg(F.sum("amount").alias("amount"))
    # Apply filter on deposit type and optional filter on transaction channel id
    if filter is None:
        df_dt_sum_deposit = df_dt_sum.filter(F.col("type") == "DEPOSIT")
    else:
        deposit_sum_filter = F.col("transaction_details_transaction_channel_id").rlike(
            "|".join(filter)
        )
        df_dt_sum_deposit = df_dt_sum.filter(
            (F.col("type") == "DEPOSIT") & deposit_sum_filter
        )
    logger.info("deposit transaction sum done.")
    return df_dt_sum_deposit


# UDF function to handle the transposition logic
def transpose_ftd_account_values2(name_col, value_col, new_col, currency_col=None):
    """
    Transposes values based on the ftd account names per each row of a dataframe.
    Returns either dates or None.
    """
    new_col = str(new_col)
    # value_col = str(value_col) if value_col is not None else 'N/A'
    # Check if value_col is a valid date; convert if necessary
    if isinstance(value_col, str):
        try:
            # Adjust format as per your input
            value_col = datetime.strptime(value_col, "%Y-%m-%d")
        except ValueError:
            return None  # Return None if the date format doesn't match
    if name_col == "GBP Fixed Deposit Account" and "gbp_ftd" in new_col:
        return value_col
    elif (
        name_col == "GBP Fixed Deposit Account Standard"
        and "gbp_standard_ftd" in new_col
    ):
        return value_col
    elif (
        name_col == "GBP Fixed Deposit Account Exclusive"
        and "gbp_exclusive_ftd" in new_col
    ):
        return value_col
    elif name_col == "EUR Fixed Deposit Account" and "eur_ftd" in new_col:
        return value_col
    elif (
        name_col == "EUR Fixed Deposit Account Standard"
        and "eur_standard_ftd" in new_col
    ):
        return value_col
    elif (
        name_col == "EUR Fixed Deposit Account Exclusive"
        and "eur_exclusive_ftd" in new_col
    ):
        return value_col
    elif name_col == "USD Fixed Deposit Account" and "usd_ftd" in new_col:
        return value_col
    elif (
        name_col == "USD Fixed Deposit Account Standard"
        and "usd_standard_ftd" in new_col
    ):
        return value_col
    elif (
        name_col == "USD Fixed Deposit Account Exclusive"
        and "usd_exclusive_ftd" in new_col
    ):
        return value_col
    else:
        return None  # Returning None for non-matching rows


# Register the UDF
# Assuming it returns a string (date), modify if needed
transpose_udf = udf(transpose_ftd_account_values2, DateType())

# Define the function to apply the UDF to transpose values


def apply_transpose2(df, transpose_cols, *args):
    """
    Transpose values to new columns based on a column list.
    Takes as input transpose functions that apply different logic.
    """
    # The first argument (always exists)
    value_col = args[0]
    # Loop through the columns we want to transpose
    for new_col in transpose_cols:
        if len(args) > 1:
            currency_col_name = args[1]
            # Add a new column by calling the UDF for each transpose column
            df = df.withColumn(
                new_col,
                transpose_udf(
                    F.col("name"),
                    F.col(value_col),
                    lit(new_col),
                    F.col(currency_col_name),
                ),
            )
        else:
            df = df.withColumn(
                new_col, transpose_udf(F.col("name"), F.col(value_col), lit(new_col))
            )
    return df


def apply_transpose(df, transpose_cols, transpose_func, *args):
    """
    Transpose values to new columns based on a column list
    Takes as input transpose functions that apply different logic
    """

    def create_udf(transpose_func, *args):
        """
        Creates a UDF from the given transpose function
        """

        def udf_func(*cols):
            row = dict(zip(cols_names, cols))
            return transpose_func(row, *args)

        return udf(udf_func, FloatType())

    cols_names = df.columns
    for new_col in transpose_cols:
        if len(args) > 1:
            value_col, currency_col_name = args
            transpose_udf = create_udf(
                transpose_func, new_col, value_col, currency_col_name
            )
        else:
            value_col = args[0]
            transpose_udf = create_udf(transpose_func, new_col, value_col)
        df = df.withColumn(new_col, transpose_udf(*[df[col] for col in cols_names]))
    return df


def deposit_by_currency_totals(df_dt_sum_deposit, inbound_cols):
    """Aggregate deposit values by customer and account holder, summing the values"""
    customer_acc_keys = ["customer_id", "account_holder_key"]
    all_deposit_cols = customer_acc_keys + inbound_cols
    # Select relevant columns
    df_deposit_select = df_dt_sum_deposit.select(*all_deposit_cols)
    # Sum up the values by customer and account holder
    df_deposit_ccy_total = df_deposit_select.groupBy(customer_acc_keys).agg(
        *[F.sum(col).alias(col) for col in inbound_cols]
    )
    logger.info("deposit by currency totals done.")
    return df_deposit_ccy_total


# UDF for transposing currency values


def transpose_currencies_values(row, new_col, value_col, ccy_code):
    """
    Transposes currency amounts or balances based on the
    currency codes per each row of a dataframe
    """
    # Define a mapping for currency codes and columns
    ccy_mapping = {
        "GBP": "gbp",
        "USD": "usd",
        "EUR": "eur",
        "AED": "aed",
        "SAR": "sar",
        "KWD": "kwd",
    }
    # Iterate through currency mapping and apply logic
    for currency, col_suffix in ccy_mapping.items():
        if currency in row[ccy_code] and col_suffix in new_col:
            return float(row[value_col])
    return None


def build_account_balances(
    df_deposit_accounts: DataFrame, balance_cols: list, ftd_balance_cols: list
) -> DataFrame:
    """
    ETL process to build all account types balance data using PySpark
    """
    df_last_balance = end_of_day_deposit_balances(df_deposit_accounts)
    df_last_balance = apply_transpose(
        df_last_balance,
        balance_cols,
        transpose_currencies_values,
        "balances_total_balance",
        "name",
    )
    df_last_balance = apply_transpose(
        df_last_balance,
        ftd_balance_cols,
        transpose_ftd_account_values,
        "balances_total_balance",
    )
    df_acc_balance = ftd_balances(df_last_balance, balance_cols, ftd_balance_cols)
    logger.info("building account balances... ")
    return df_acc_balance


def transpose_ftd_account_values(row, new_col, value_col):
    """
    Transposes values based on the ftd account names per each row of a dataframe in PySpark
    Returns either a date or a float
    """
    account_name = row["name"]
    if account_name == "GBP Fixed Deposit Account" and "gbp_ftd" in new_col:
        return row[value_col]
    elif (
        account_name == "GBP Fixed Deposit Account Standard"
        and "gbp_standard_ftd" in new_col
    ):
        return row[value_col]
    elif (
        account_name == "GBP Fixed Deposit Account Exclusive"
        and "gbp_exclusive_ftd" in new_col
    ):
        return row[value_col]
    elif account_name == "EUR Fixed Deposit Account" and "eur_ftd" in new_col:
        return row[value_col]
    elif (
        account_name == "EUR Fixed Deposit Account Standard"
        and "eur_standard_ftd" in new_col
    ):
        return row[value_col]
    elif (
        account_name == "EUR Fixed Deposit Account Exclusive"
        and "eur_exclusive_ftd" in new_col
    ):
        return row[value_col]
    elif account_name == "USD Fixed Deposit Account Exclusive" and "usd_ftd" in new_col:
        return row[value_col]
    elif (
        account_name == "USD Fixed Deposit Account Standard"
        and "usd_standard_ftd" in new_col
    ):
        return row[value_col]
    elif (
        account_name == "USD Fixed Deposit Account Exclusive"
        and "usd_exclusive_ftd" in new_col
    ):
        return row[value_col]
    else:
        return None if value_col == "balances_total_balance" else None


def end_of_day_deposit_balances(df_deposit_accounts: DataFrame) -> DataFrame:
    """
    Filter by end of day balance using PySpark
    """
    df_balance = df_deposit_accounts.select(
        "account_holder_key", "name", "balances_total_balance", "last_modified_date"
    )
    df_last_balance = df_balance.groupBy(
        "account_holder_key", "name", "balances_total_balance"
    ).agg(F.max("last_modified_date").alias("last_modified_date"))
    # Apply Window function to get the max last_modified_date for each name
    window_spec = Window.partitionBy("name").orderBy(F.desc("last_modified_date"))
    df_last_balance = df_last_balance.withColumn(
        "row_number", F.row_number().over(window_spec)
    )
    df_last_balance = df_last_balance.filter(F.col("row_number") == 1)
    df_last_balance = df_last_balance.drop("row_number")
    return df_last_balance


def ftd_balances(
    df_last_balance: DataFrame, balance_cols: list, ftd_balance_cols: list
) -> DataFrame:
    """
    Filter for ftd balances using PySpark
    """
    # Select relevant columns
    df_cols = ["account_holder_key"] + balance_cols + ftd_balance_cols
    df_acc_balance = df_last_balance.select(df_cols)
    # Group by account_holder_key and sum the balance columns
    df_acc_balance_1 = df_acc_balance.groupBy("account_holder_key").agg(
        *[F.sum(col).alias(col) for col in balance_cols + ftd_balance_cols]
    )
    return df_acc_balance_1


def build_maturity_dates(df, df_deposit_accounts, maturity_cols):
    "ETL process to build all maturity dates for all account types"
    df_c_maturity = deposit_account_maturities(df, df_deposit_accounts)
    df_c_maturity = apply_transpose2(df_c_maturity, maturity_cols, "maturity_date")
    df_maturity_final = ftd_account_maturity_dates(df_c_maturity, maturity_cols)
    logger.info("building account maturity dates...")
    return df_maturity_final


def deposit_account_maturities(df, df_deposit_accounts):
    "Get maturity dates per account types"
    # Select necessary columns and drop rows with null values
    df_maturity = df_deposit_accounts.select(
        "account_holder_key", "name", "maturity_date"
    ).dropna()
    # Inner join between df and df_maturity on account_holder_key and name
    df_c_maturity_date = df.join(df_maturity, ["account_holder_key", "name"], "inner")
    # Convert 'maturity_date' to Spark DateType
    df_c_maturity_date = df_c_maturity_date.withColumn(
        "maturity_date", to_date(col("maturity_date"))
    )
    # Group by customer_id, account_holder_key, and name and get the max maturity_date
    df_c_maturity = df_c_maturity_date.groupBy(
        "customer_id", "account_holder_key", "name"
    ).agg(F.max("maturity_date").alias("maturity_date"))
    return df_c_maturity


def ftd_account_maturity_dates(df_c_maturity, maturity_cols):
    """
    Filter for FTD maturity dates.
    Args:
        df_c_maturity (DataFrame): Input DataFrame with customer_id and maturity columns.
        maturity_cols (list): List of column names to find maximum values.
    Returns:
        DataFrame: DataFrame with the maximum maturity dates for each customer_id.
    """
    # Group by customer_id and compute max for each maturity column
    df_maturity_final = df_c_maturity.groupBy("customer_id").agg(
        *[F.max(col).alias(col) for col in maturity_cols]
    )
    return df_maturity_final


def build_account_opening_dates(df, ftd_account_opening_cols):
    df_customer_acc_opening = customer_account_openings(df)
    df_customer_acc_opening = apply_transpose2(
        df_customer_acc_opening, ftd_account_opening_cols, "approved_date"
    )
    df_customer_acc_opening_final = ftd_account_approved_dates(
        df_customer_acc_opening, ftd_account_opening_cols
    )
    return df_customer_acc_opening_final


def customer_account_openings(df):
    """
    Get customer first account opening date using PySpark equivalent of groupby + min.
    """
    df = df.withColumn("approved_date", to_date(col("approved_date")))
    # Group by 'customer_id', 'account_holder_key', 'name' and get the minimum 'approved_date'
    df_customer_acc_opening = df.groupBy(
        "customer_id", "account_holder_key", "name"
    ).agg(F.min("approved_date").alias("approved_date"))
    return df_customer_acc_opening


def ftd_account_approved_dates(df_customer_acc_opening, ftd_account_opening_cols):
    """
    Filter for FTD approved dates and select the maximum for each account opening column per customer.
    """
    # Create a list of all columns to be selected (customer_id + ftd account columns)
    all_ftd_account_opening_cols = ["customer_id"] + ftd_account_opening_cols
    # Select only the required columns from the input DataFrame
    df_customer_acc_opening = df_customer_acc_opening.select(
        *all_ftd_account_opening_cols
    )
    # Initialize an empty DataFrame to store max values
    df_max_values = None
    # Iterate through each column in ftd_account_opening_cols
    for col in ftd_account_opening_cols:
        # Create a window partitioned by 'customer_id'
        window_spec = Window.partitionBy("customer_id")
        # Calculate the maximum value for the current column within the customer_id partition
        df_col_max = df_customer_acc_opening.withColumn(
            col, F.max(col).over(window_spec)
        )
        # Select the 'customer_id' and the max column value
        df_col_max = df_col_max.select("customer_id", col).distinct()
        # Join with the previous results, or assign if df_max_values is None
        if df_max_values is None:
            df_max_values = df_col_max
        else:
            df_max_values = df_max_values.join(
                df_col_max, on="customer_id", how="inner"
            )
    return df_max_values


def incentive_reports(s3_bucket, final_cols, date_cols, *args):
    """
    Joins customer, deposit account, and transaction data,
    and also joins transposed balances, payment, and dates.
    Parameters:
    1. s3_bucket: name of the S3 bucket to save the report to
    2. path_and_file: name of S3 bucket path and file name
    3. final_cols: list of the final selection of column names
    4. date_cols: list of columns containing dates
    5. args: multiple dataframes covering customers, deposits, and transactions
    """
    # Unpack input dataframes
    df_customers = args[0]
    df_deposit_ccy_total = args[1]
    df_first_dates = args[2]
    df_sc = args[3]
    df_acc_balance = args[4]
    df_maturity_final = args[5]
    df_last_customer_transaction = args[6]
    df_customer_acc_opening_final = args[7]
    df_first_customer_approved_dt = args[8]
    # Group by customer_id in df_customers
    df_all_customers_ids = df_customers.groupBy("customer_id").agg(
        F.count("*").alias("count")
    )
    # Perform left joins, replacing pd.merge with Spark's .join()
    dfy = df_all_customers_ids.join(df_deposit_ccy_total, on="customer_id", how="left")
    df1 = dfy.join(df_first_dates, on="customer_id", how="left")
    df2 = df1.join(df_sc, on="customer_id", how="left")
    df3 = df2.join(df_acc_balance, on="account_holder_key", how="left")
    df4 = df3.join(df_maturity_final, on="customer_id", how="left")
    df5 = df4.join(df_last_customer_transaction, on="customer_id", how="left")
    df6 = df5.join(df_customer_acc_opening_final, on="customer_id", how="left")
    df7 = df6.join(df_first_customer_approved_dt, on="customer_id", how="left")
    # logger.info("Merging account types, balances, and dates...")
    # Select only the columns that exist in df7 from final_cols
    selected_columns = [col for col in final_cols if col in df7.columns]
    df7 = df7.select(*selected_columns)
    # Handle date columns: convert to string format "dd/MM/yyyy HH:mm"
    selected_date_columns = [col for col in date_cols if col in df7.columns]
    for date_col in selected_date_columns:
        df7 = df7.withColumn(
            date_col, F.date_format(F.to_timestamp(df7[date_col]), "dd/MM/yyyy HH:mm")
        )
    # Round balance and payment columns to 2 decimal places
    balance, payment = "balance", "payment"
    selected_amounts = [col for col in df7.columns if balance in col or payment in col]
    for amt_col in selected_amounts:
        df7 = df7.withColumn(amt_col, F.round(df7[amt_col], 2))
    logger.info("Building SFMC incentive report data...")
    # Save the final dataframe to S3 in CSV format
    write_as_csv_s3(df7, s3_bucket)
    return df7


def write_as_csv_s3(df: DataFrame, s3_bucket: str):
    """
    Writes the DataFrame to a CSV file in the specified S3 bucket.
    """
    path_and_file = "data-lake/sfmc_incentive_data"
    output_path = f"s3://{s3_bucket}/{path_and_file}"
    df.coalesce(1).write.mode("overwrite").csv(output_path, header=True)


def rename_part_file_in_s3(s3_bucket):
    # Initialize the S3 client
    s3 = boto3.client("s3")

    # Specify the S3 bucket and file paths
    path_and_file = "data-lake/sfmc_incentive_data/"
    new_filename = "data-lake/sfmc_incentive_data.csv"
    output_dir = f"s3://{s3_bucket}/{path_and_file}"

    # Use boto3 to list objects in the S3 directory
    response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=path_and_file)

    # Check if files exist in the directory
    if "Contents" in response:
        for obj in response["Contents"]:
            file_name = obj["Key"]
            if file_name.startswith(path_and_file + "part-") and file_name.endswith(
                ".csv"
            ):
                part_file = file_name
                new_file = new_filename

                # Copy the part file to a new location with the desired name
                s3.copy_object(
                    Bucket=s3_bucket,
                    CopySource={"Bucket": s3_bucket, "Key": part_file},
                    Key=new_file,
                )
                print(f"Renamed {part_file} to {new_file} in S3")
    else:
        print(f"No files found in {output_dir}")


if __name__ == "__main__":
    # @params: [JOB_NAME]
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_RAW", "s3bucket"])
    s3_bucket_raw = args["S3_RAW"]
    s3_bucket = args["s3bucket"]

    # sc = SparkContext()
    # glueContext = GlueContext(sc)
    # spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    spark.conf.set("spark.sql.parquet.mergeSchema", "true")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    input_database = "datalake_raw"
    df_customers = get_customer_data(
        customers_sql,
        input_database,
        customer_id1="dynamodb_keys_id_s",
        customer_id2="dynamodb_new_image_id_s",
    ).cache()

    df_start_customers = df_customers.filter(
        df_customers["dynamodb_new_image_status_s"] == "AWAITING_MANUAL_REVIEW"
    )

    df_deposit_accounts = get_deposit_data(deposit_accounts_sql, input_database).cache()
    # df_deposit_accounts.printSchema()

    df_clients = get_client_data(clients_sql, input_database)

    df_deposit_transactions = get_deposit_data(deposit_transactions_sql, input_database)

    # Example of using the function
    df_1 = merge_customer_and_deposit_data(
        df_customers,
        df_clients,
        df_deposit_accounts,
        df_deposit_transactions,
        customer_sort_list,
        client_sort_list,
        account_sort_list,
        transaction_sort_list,
        customer_group_list,
        client_group_list,
        account_group_list,
        transaction_group_list,
        customer_transaction_columns,
    ).cache()

    df_sc = build_submission_dates(
        df_start_customers, customer_sort_list, customer_group_list
    )

    df_first_dates = first_deposit_transactions(df_1, channel_filter_list)

    df_max_booking = last_deposit_transactions(df_1)

    df_last_customer_transaction = last_debit_transaction(df_1)

    df_first_customer_approved_dt = first_customer_approved_dates(df_1)

    df_deposit_ccy_total = build_inbound_deposit_values(
        df_1, inbound_cols, channel_filter_list
    )

    df_acc_balance = build_account_balances(
        df_deposit_accounts, balance_cols, ftd_balance_cols
    )

    df_maturity_final = build_maturity_dates(df_1, df_deposit_accounts, maturity_cols)

    df_customer_acc_opening_final = build_account_opening_dates(
        df_1, ftd_account_opening_cols
    )

    incentive_reports(
        s3_bucket,
        final_cols,
        date_cols,
        df_customers,
        df_deposit_ccy_total,
        df_first_dates,
        df_sc,
        df_acc_balance,
        df_maturity_final,
        df_last_customer_transaction,
        df_customer_acc_opening_final,
        df_first_customer_approved_dt,
    )
    rename_part_file_in_s3(s3_bucket)

    df_customers.unpersist()
    df_start_customers.unpersist()
    df_deposit_accounts.unpersist()
    df_1.unpersist()
    job.commit()
