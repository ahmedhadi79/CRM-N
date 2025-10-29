import os

from api_client import APIClient
from custom_functions import fill_milliseconds
from custom_functions import logger
from custom_functions import raw_load_to_s3
from data_catalog import column_comments
from data_catalog import schemas

table_name = "mc_event_definitions"

# Platform Meta data that is not needed to be saved.
keys_to_remove = [
    "schema",
    "metaData",
    "schedule",
    "arguments",
    "iconUrl",
    "filterDefinitionTemplate",
    "configurationArguments",
]


def loop_all_pages(first_payload, api_instance):
    data_list = []
    payload = first_payload["items"]
    total_records = int(first_payload["count"])
    total_pages = total_records // 50
    if total_records % 50 != 0:
        total_pages += 1
    page = 1
    while True:
        data_list.extend(payload)
        page += 1

        if page <= total_pages:
            payload = api_instance.get(
                endpoint="interaction/v1/eventDefinitions/",
                query="$page="
                + str(page)
                + "+&$pagesize=50&$orderBy=ModifiedDate+DESC",
                filter_objects=["items"],
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
        endpoint="interaction/v1/eventDefinitions/",
        query="$page=1&$pagesize=50&$orderBy=ModifiedDate+DESC",
        filter_objects=["count", "items"],
        clean=True,
    )

    all_pages = loop_all_pages(page1, mc_api)

    logger.info("Removing unnecessary fields: " + str(keys_to_remove))
    for record in all_pages:
        for key in keys_to_remove:
            record.pop(key, None)

    ed_df = APIClient.process_response(
        mc_api, response=all_pages, flatten=True, df=True
    )
    ed_df = fill_milliseconds(ed_df, table_name, schemas)

    logger.info("Saving results..")
    res = raw_load_to_s3(
        ingested_df=ed_df,
        table_name=table_name,
        column_comments=column_comments,
        schemas=schemas,
        env=os.environ["ENV"],
        mode="overwrite",
        file_type="parquet",
        no_partition=True,
    )

    return res
