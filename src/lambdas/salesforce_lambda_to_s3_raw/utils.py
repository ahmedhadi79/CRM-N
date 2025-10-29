import logging
import re
import sys
from datetime import date
from datetime import datetime
from datetime import timezone
from typing import Literal
from typing import Optional

import awswrangler as wr
import boto3
import pandas as pd
from flatten_json import flatten


def setup_logger(
    name: Optional[str] = None,
    level: int = logging.INFO,
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    filename: Optional[str] = None,
) -> logging.Logger:
    """
    Sets up a logger with the specified configuration.

    Parameters:
    - name (Optional[str]): Name of the logger. If None, the root logger is used.
    - level (int): Logging level (e.g., logging.INFO, logging.DEBUG).
    - format (str): Log message format.
    - filename (Optional[str]): If specified, logs will be written to this file. Otherwise, logs are written to stdout.

    Returns:
    - logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if filename:
        handler = logging.FileHandler(filename)
    else:
        handler = logging.StreamHandler(sys.stdout)

    handler.setLevel(level)
    formatter = logging.Formatter(format)
    handler.setFormatter(formatter)

    # To avoid duplicate handlers being added
    if not logger.hasHandlers():
        logger.addHandler(handler)

    return logger


logger = setup_logger("utils.functions")


def get_salesforce_data(data, salesforce_api):
    data_list = []
    current_data = data  # Assuming 'data' is the initial response

    while True:
        # Append the current batch of records to your list
        data_list.append(current_data["records"])

        # Check if there's a next page to fetch
        if "nextRecordsUrl" in current_data:
            # Fetch the next page
            next_url = current_data["nextRecordsUrl"] + "/"
            current_data = salesforce_api.get(
                endpoint=next_url,
                filter_objects=["nextRecordsUrl", "records"],
                clean=True,
            )
        else:
            # No more pages to fetch, break out of the loop
            break
    return data_list


def get_salesforce_df(data_list):
    flattened_data = [flatten(record) for sublist in data_list for record in sublist]
    # Convert the flattened list of records into a DataFrame
    df = pd.DataFrame(flattened_data)
    df["date"] = date.today().strftime("%Y%m%d")
    df["timestamp_extracted"] = datetime.now(timezone.utc)
    columns_to_drop = [
        "attributes_type",
        "attributes_url",
    ]

    # Drop columns only if they exist in the DataFrame
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
    return df


# Convert camelCase to snake_case
def camelcase_to_snake_case(df):
    columns = []
    for col in df.columns:
        x = re.sub("(?!^)([A-Z]+)", r"_\1", col).lower()
        columns.append(x)
    df.columns = columns
    return df


def add_meta_columns(df: pd.DataFrame, cdc_field: str):
    if cdc_field:
        cdc_field = cdc_field.replace(".", "_").lower()
        df[cdc_field] = apply_iso_format(df[cdc_field])
        df["date"] = df[cdc_field].dt.strftime("%Y%m%d")
    else:
        df["date"] = date.today().strftime("%Y%m%d")

    df["timestamp_extracted"] = datetime.now(timezone.utc)
    return df


def raw_load_to_s3(
    ingested_df: pd.DataFrame,
    table_name: str,
    env: Literal["sandbox", "alpha", "beta", "prod"],
    file_type: Literal["csv", "json", "parquet"],
    mode: Literal["append", "overwrite", "overwrite_partitions"],
    column_comments: dict = None,
    schemas: dict = None,
    filtered_columns: list[str] = None,
    rows_chunk: int = 400000,
    no_partition: bool = False,
    boto3_session: boto3.Session = None,
):
    """
    Custom wrapper built over wrangler save functions
    to load a DataFrame to datalake S3 bucket in a specified file format.

    Parameters:
    - ingested_df (pd.DataFrame): The DataFrame to be loaded to S3.
    - table_name (str): The name of the table or object in the S3 bucket.
    - column_comments (dict): A dictionary containing column_comments.
    - schemas (dict): A dictionary containing schemas.
    - env (Literal["sandbox", "alpha", "beta", "prod"]): The environment in which the data is being loaded.
    - file_type (Literal["csv", "json", "parquet"]): The file format in which the data will be stored.
    - mode (Literal["append", "overwrite", "overwrite_partitions"]).
    - filtered_columns (list[str], optional): List of columns to include in the output. Default is None (all columns).
    - rows_chunk (int, optional): Number of rows to be written in each chunk. Default is 400,000.
    - no_partition (bool, optional): If True, data is stored without partitioning. Default is False.
    - boto3_session (boto3.Session, optional): A custom boto3 session. Default is None.
    """
    target_bucket_name = f"bb2-{env}-datalake-raw"
    path = f"s3://{target_bucket_name}/{table_name}/"
    logger.info(
        f"[load_to_s3]: Uploading to S3 path s3://{target_bucket_name}/{table_name}/"
    )

    if filtered_columns:
        ingested_df = ingested_df[
            [col for col in ingested_df.columns if col in filtered_columns]
        ]

    logger.info("Dataframe shape:  %s", ingested_df.shape)

    try:
        if file_type == "parquet":
            wr.s3.to_parquet(
                df=ingested_df,
                path=path,
                database="datalake_raw",
                table=table_name,
                partition_cols=None if no_partition else ["date"],
                mode=mode,
                max_rows_by_file=rows_chunk,
                use_threads=True,
                index=False,
                dataset=True,
                schema_evolution=True,
                compression="snappy",
                dtype=schemas[table_name],
                glue_table_settings=wr.typing.GlueTableSettings(
                    columns_comments=column_comments[table_name]
                ),
                boto3_session=boto3_session,
            )
        elif file_type == "csv":
            wr.s3.to_csv(
                df=ingested_df,
                path=path,
                database="datalake_raw",
                table=table_name,
                partition_cols=None if no_partition else ["date"],
                mode=mode,
                max_rows_by_file=rows_chunk,
                use_threads=True,
                index=False,
                dataset=True,
                schema_evolution=True,
                dtype=schemas[table_name],
                glue_table_settings=wr.typing.GlueTableSettings(
                    columns_comments=column_comments[table_name]
                ),
                escapechar="\\",
                boto3_session=boto3_session,
            )
        elif file_type == "json":
            wr.s3.to_json(
                df=ingested_df,
                path=path,
                database="datalake_raw",
                table=table_name,
                use_threads=True,
                lines=True,
                date_format="iso",
                orient="records",
                index=False,
                dataset=True,
                boto3_session=boto3_session,
            )
        else:
            raise ValueError(
                "Invalid file_type. Supported types are 'csv', 'json', and 'parquet'."
            )

        logger.info(f"[Sucess]: Uploaded to s3://{target_bucket_name}/{table_name}/")
    except Exception as e:
        logger.error("Athena schema:  %s", schemas[table_name])
        logger.error(
            msg=f"Failed uploading to S3 path s3://{target_bucket_name}/{table_name}/"
        )
        raise e


def make_query(sql):
    logger.info(f"Executing query: {sql}")
    return wr.athena.read_sql_query(
        sql=sql,
        database="datalake_raw",
        workgroup="datalake_workgroup",
        ctas_approach=False,
        keep_files=False,
    )


def get_start_time_from_athena(table_name: str, cdc_field: str):
    """
    Fetches the latest created date from the Athena table.
    If the table does not exist, returns None.
    Output format: 'YYYY-MM-DDTHH:MM:SS.000Z'
    """
    try:
        cdc_field = cdc_field.replace(".", "_").lower()
        # Query Athena for the latest created date
        query = f"""
        SELECT MAX({cdc_field})
        FROM datalake_raw.{table_name}
        """
        logger.info("Executing Athena query to fetch start_time")
        result = make_query(query)

        if not result.empty:
            latest_creation_date = result["_col0"].iloc[0]
            if latest_creation_date is not None:
                if not isinstance(latest_creation_date, pd.Timestamp):
                    try:
                        latest_creation_date = pd.Timestamp(latest_creation_date)
                    except Exception as e:
                        logger.error(f"Error parsing {cdc_field}: {e}")
                        raise ValueError(
                            f"Invalid datetime format for {cdc_field}: {latest_creation_date}"
                        )

                # Convert to UTC and format as ISO 8601 with milliseconds and 'Z'
                latest_creation_date = latest_creation_date.replace(
                    tzinfo=timezone.utc
                ).strftime("%Y-%m-%dT%H:%M:%S.000Z")

                logger.info(f"Latest {cdc_field} from Athena: {latest_creation_date}")

                return latest_creation_date

    except Exception as e:
        logger.error(f"Error fetching start_time from Athena: {e}")
        raise e


def apply_iso_format(timestamp_column: pd.Series) -> pd.Series:
    """
    Apply ISO format to a timestamp column, trying multiple formats for each record.

    Args:
        timestamp_column (pd.Series): Series with timestamp data to be processed.

    Returns:
        pd.Series: Series with ISO formatted timestamps.
    """
    # Define the list of date formats to try
    date_formats = [
        "ISO8601",  # ISO8601 format
        "%Y%m%d%H%M",  # Ex: 202409090450
        "%b %d, %Y, %I:%M:%S %p",  # Ex: Sep 16, 2024, 03:41:17 AM
        "%m/%d/%Y %I:%M:%S %p",  # Ex: 7/30/2024 6:27:00 PM
        "%Y-%m-%d %H:%M:%S",  # Ex: 2024-07-30 18:27:00
        "%d-%m-%Y %H:%M:%S",  # Ex: 30-07-2024 18:27:00
    ]

    def parse_date(date_str):
        for date_format in date_formats:
            try:
                return pd.to_datetime(
                    date_str, format=date_format, utc=True, errors="raise"
                )
            except (ValueError, TypeError):
                continue
        raise ValueError(
            f"Error processing date {date_str} in {timestamp_column.name}: Unable to parse date with provided formats"
        )

    return timestamp_column.apply(parse_date)


def apply_schema(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    """
    Apply specified data types to the columns of a DataFrame based on the input schema

    Args:
        df (pd.DataFrame): DataFrame with generic data types.
        schema (dict): athena df schema containing columns' dtypes

    Returns:
        pd.DataFrame: DataFrame with data types specified in the schema.
    """

    # Mapping schema types to pandas dtypes
    schema_type_mapping = {
        "int": "int32",
        "bigint": "int64",
        "string": "string[python]",
        "timestamp": "datetime64[ns]",
        "double": "float64",
        "boolean": "bool",
        "date": "datetime64[ns]",
    }

    # Lowercase column names before mapping
    df.columns = df.columns.str.lower()
    schema = {key.lower(): value for key, value in schema.items()}

    for column, dtype in schema.items():
        if column in df.columns:
            pandas_dtype = schema_type_mapping.get(dtype, "string[python]")
            if dtype in ["timestamp", "date"] and column != "date":
                df[column] = apply_iso_format(df[column])
            elif dtype in ["int", "bigint", "double"]:
                df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0)
            else:
                df[column] = df[column].astype(pandas_dtype)

    return df
