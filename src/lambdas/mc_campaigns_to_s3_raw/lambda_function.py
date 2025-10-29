import os

from api_client import APIClient
from custom_functions import logger
from custom_functions import raw_load_to_s3
from data_catalog import column_comments
from data_catalog import schemas

table_name = "mc_campaigns"


def loop_all_pages(first_payload, api_instance):
    data_list = []
    payload = first_payload

    while True:
        data_list.extend(payload["items"])

        if "next" in payload["links"]:
            # Fetch the next page
            next_url = "hub" + payload["links"]["next"]["href"]
            payload = api_instance.get(
                endpoint=next_url,
                filter_objects=["links", "items"],
                clean=True,
            )
        else:
            break
    return data_list


def lambda_handler(event, context):
    logger.info("Initializing API handler..")
    mc_api = APIClient(
        login_url=os.environ["LOGIN_URL"],
        auth=os.environ["AUTH_PATH"],
        secrets_manager=True,
    )

    page1 = mc_api.get(
        endpoint="hub/v1/campaigns/",
        query="$page=1&$pagesize=50&extras=all",
        filter_objects=["links", "items"],
        clean=True,
    )

    data_list = loop_all_pages(page1, mc_api)

    campaigns_df = APIClient.process_response(mc_api, response=data_list, df=True)

    logger.info("Saving results..")
    res = raw_load_to_s3(
        ingested_df=campaigns_df,
        table_name=table_name,
        column_comments=column_comments,
        schemas=schemas,
        env=os.environ["ENV"],
        mode="overwrite",
        file_type="parquet",
        no_partition=True,
    )

    return res
