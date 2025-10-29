import os

from api_client import APIClient
from custom_functions import logger
from custom_functions import raw_load_to_s3
from data_catalog import column_comments
from data_catalog import schemas

table_name = "mc_data_extensions_list"


def lambda_handler(event, context):
    logger.info("Initializing API handler..")
    mc_api = APIClient(
        login_url=os.environ["LOGIN_URL"],
        auth=os.environ["AUTH_PATH"],
        secrets_manager=True,
    )

    de_df = mc_api.get(
        endpoint="legacy/v1/beta/object/", filter_objects=["entry"], clean=True, df=True
    )

    # Fix isPublic type Object > Bool
    de_df["isPublic"] = de_df["isPublic"].map({"True": True, "False": False})
    de_df["isPublic"] = de_df["isPublic"].astype(bool)

    logger.info("Saving results..")
    res = raw_load_to_s3(
        ingested_df=de_df,
        table_name=table_name,
        column_comments=column_comments,
        schemas=schemas,
        env=os.environ["ENV"],
        mode="overwrite",
        file_type="parquet",
        no_partition=True,
    )

    return res
