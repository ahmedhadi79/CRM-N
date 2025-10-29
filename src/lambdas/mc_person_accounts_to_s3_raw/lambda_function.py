import os
import time
import requests
import awswrangler as wr
import pandas as pd
from api_client import APIClient
from custom_functions import logger


def loop_all_pages(first_payload, api_instance, de_key):
    data_list = []
    payload = first_payload["items"]
    total_pages = int(first_payload["count"]) // 2500
    if (int(first_payload["count"]) % 2500) != 0:
        total_pages += 1
    page = 1

    while True:
        data_list.extend(payload)
        page += 1

        if page <= total_pages:
            retries = 3
            delay = 5
            for attempt in range(retries):
                try:
                    payload = api_instance.get(
                        endpoint=f"data/v1/customobjectdata/key/{de_key}/rowset",
                        query=f"$page={page}&$pagesize=2500",
                        filter_objects=["items"],
                        clean=True,
                    )
                    break
                except requests.HTTPError as e:
                    if e.response.status_code == 500:
                        logger.warning(f"500 error on page {page}, attempt {attempt + 1}/{retries}")
                        time.sleep(delay)
                        delay *= 2
                    else:
                        logger.error(f"HTTP error on page {page}: {e}")
                        raise
            else:
                logger.error(f"Failed to retrieve page {page} after {retries} attempts. Skipping.")
                break
        else:
            break
    return data_list


def save_to_parquet_s3(all_pages, bucket_name, table_name):
    df = pd.DataFrame(all_pages)

    abs_path = f"s3://{bucket_name}/{table_name}/{table_name}.parquet"

    logger.info("Uploading parquet file to " + abs_path)
    wr.s3.to_parquet(df, abs_path)
    return logger.info("[Sucess]: Uploaded to " + abs_path)


def lambda_handler(event, context):
    logger.info("Initializing API handler..")
    mc_api = APIClient(
        login_url=os.environ["LOGIN_URL"],
        auth=os.environ["AUTH_PATH"],
        secrets_manager=True,
    )

    # Key for SFMC Data Extension "DE_Master_Salesforce_PersonAccounts"
    de_key = "1DB286F8-4FBE-49F6-A452-FA257C2F3E81"

    page1 = mc_api.get(
        endpoint=f"data/v1/customobjectdata/key/{de_key}/rowset",
        query="$page=1&$pagesize=2500",
        filter_objects=["count", "items"],
        clean=True,
    )

    all_pages = loop_all_pages(page1, mc_api, de_key)
    all_pages = [row["values"] for row in all_pages]

    env = os.environ["ENV"]
    bucket_name = f"bb2-{env}-datalake-raw"
    table_name = "mc_person_accounts"

    res = save_to_parquet_s3(all_pages, bucket_name, table_name)
    return res
