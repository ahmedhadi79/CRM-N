import os
from datetime import datetime

from api_client import APIClient
from data_catalog import column_comments
from data_catalog import schemas
from sf_all_queries import queries

from utils import add_meta_columns
from utils import apply_schema
from utils import get_salesforce_data
from utils import get_salesforce_df
from utils import get_start_time_from_athena
from utils import logger
from utils import raw_load_to_s3

logger.info("Saving results..")


def lambda_handler(event, context):
    try:
        logger.info(f"Received event {event}")
        logger.info("Initializing API handler..")

        salesforce_api = APIClient(
            login_url=os.environ["TOKEN_URL"],
            auth=os.environ["SALESFORCE_AUTH_DETAILS"],
            secrets_manager=True,
        )
        sf_version = os.environ["SALESFORCE_API_VERSION"]
        table_name = event["table_name"]
        cdc_field = event.get("cdc_field")

        if event.get("start_date"):
            try:
                # Convert to datetime object
                start_dt_obj = datetime.strptime(
                    event["start_date"], "%Y-%m-%d %H:%M:%S.%f"
                )

                # Format to the desired format with 'Z' at the end
                start_date = start_dt_obj.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            except ValueError as e:
                logger.error(
                    f"Invalid start_date format: {event['start_date']}, Error: {str(e)}"
                )
                return {"error": "Invalid start_date format"}

        else:
            start_date = get_start_time_from_athena(table_name, cdc_field)

        if event.get("end_date"):
            try:
                # Convert to datetime object
                end_dt_obj = datetime.strptime(
                    event["end_date"], "%Y-%m-%d %H:%M:%S.%f"
                )

                # Format to the desired format with 'Z' at the end
                end_date_condition = f" AND {cdc_field} < {end_dt_obj.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'}"
            except ValueError as e:
                logger.error(
                    f"Invalid end_date_condition format: {event['end_date']}, Error: {str(e)}"
                )
                return {"error": "Invalid end_date_condition format"}

        else:
            end_date_condition = ""

        if event.get("extra_soql_condition"):
            extra_soql_condition = f" AND {event['extra_soql_condition']}"
        else:
            extra_soql_condition = ""

        data = salesforce_api.get(
            endpoint=f"services/data/{sf_version}/query/",
            filter_objects=["done", "nextRecordsUrl", "records"],
            clean=True,
            query=queries[table_name].format(
                cdc_field=cdc_field,
                start_date=start_date,
                end_date_condition=end_date_condition,
                extra_soql_condition=extra_soql_condition,
            ),
        )

        data_list = get_salesforce_data(data, salesforce_api)

        if not data_list or all(not sublist for sublist in data_list):
            logger.info(f"The data list is empty: {data_list}")
        else:
            df = get_salesforce_df(data_list)
            df = df.dropna(axis=1, how="all")
            df = apply_schema(df, schemas[table_name])
            df = add_meta_columns(df, cdc_field)

            raw_load_to_s3(
                ingested_df=df,
                table_name=table_name,
                column_comments=column_comments,
                schemas=schemas,
                env=os.environ["ENV"],
                file_type="parquet",
                mode="append",
            )

            res = f"[Success] loaded {table_name} with shape {df.shape}"
            logger.info(res)
            return res

    except Exception as e:
        logger.error(f"Error processing event: {str(e)}", exc_info=True)
        return {"error": f"Failed to process event: {str(e)}"}
