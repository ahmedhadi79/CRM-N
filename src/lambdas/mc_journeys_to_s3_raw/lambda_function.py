import json
import os

import awswrangler as wr
from api_client import APIClient
from custom_functions import logger

table_name = "mc_journeys"
temp_file_path = "/tmp/temp.json"
env = os.environ["ENV"]


def loop_all_pages(first_payload, api_instance):
    data_list = []
    payload = first_payload["items"]
    total_pages = int(first_payload["count"]) // 50
    if (int(first_payload["count"]) % 50) != 0:
        total_pages += 1
    page = 1

    while True:
        data_list.extend(payload)
        page += 1

        if page <= total_pages:
            payload = api_instance.get(
                endpoint="interaction/v1/interactions/",
                query="$page="
                + str(page)
                + "&$pagesize=50&extras=all&$orderBy=ModifiedDate+DESC&mostRecentVersionOnly=false",
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
        endpoint="interaction/v1/interactions/",
        query="$page=1&$pagesize=50&extras=all&$orderBy=ModifiedDate+DESC&mostRecentVersionOnly=false",
        filter_objects=["count", "items"],
        clean=True,
    )

    all_pages = loop_all_pages(page1, mc_api)
    json_data = json.dumps(all_pages)

    with open(temp_file_path, "w") as json_file:
        json_file.write(json_data)

    bucket_name = f"bb2-{env}-datalake-raw"
    abs_path = f"s3://{bucket_name}/{table_name}/{table_name}.json"

    logger.info("Uploading json file to " + abs_path)
    res = wr.s3.upload(temp_file_path, abs_path)
    os.remove(temp_file_path)
    logger.info("[Sucess]: Uploaded to " + abs_path)
    return res
