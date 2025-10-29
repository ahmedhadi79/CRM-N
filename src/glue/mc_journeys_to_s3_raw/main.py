import json
import os
import sys

import awswrangler as wr
from api_client import APIClient
from awsglue.utils import getResolvedOptions
from custom_functions import logger
from requests.exceptions import HTTPError


def initialize_client(args):
    return APIClient(
        login_url=args["LOGIN_URL"],
        auth=args["AUTH_PATH"],
        secrets_manager=True,
    )


def loop_all_pages(first_payload, api_instance, args):
    try:
        data_list = []
        payload = first_payload["items"]
        total_pages = (int(first_payload["count"]) + 49) // 50
        page = 2

        while page <= total_pages:
            data_list.extend(payload)

            if page <= total_pages:
                try:
                    payload = api_instance.get(
                        endpoint="interaction/v1/interactions/",
                        query=f"$page={page}&$pagesize=50&extras=all&$orderBy=ModifiedDate+DESC&mostRecentVersionOnly=false",
                        filter_objects=["items"],
                        clean=True,
                    )
                except HTTPError as e:
                    if e.response.status_code == 401:
                        logger.warning(
                            "401 Unauthorized error during pagination. Reinitializing API client."
                        )
                        api_instance = initialize_client(args)

                        # Retry the request with a new token
                        payload = (
                            []
                        )  # Reset cached payload to avoid extending dublications.
                        continue
                    else:
                        logger.error(f"Failed to fetch page {page}: {e}")
                        raise e
                page += 1

        return data_list
    except Exception as e:
        logger.error(f"Error during pagination: {e}")
        raise


def main():
    try:
        # Stage1: Initializing environment variables
        logger.info("Initializing API handler..")
        args = getResolvedOptions(
            sys.argv,
            [
                "ENV",
                "LOGIN_URL",
                "AUTH_PATH",
            ],
        )
        env = args["ENV"]
        table_name = "mc_journeys"
        temp_file_path = "/tmp/temp.json"

        # Stage2: Initializing client and OAuth2 authintication
        mc_api = initialize_client(args)

        # Stage3: Aquiring the first day data
        page1 = mc_api.get(
            endpoint="interaction/v1/interactions/",
            query="$page=1&$pagesize=50&extras=all&$orderBy=ModifiedDate+DESC&mostRecentVersionOnly=false",
            filter_objects=["count", "items"],
            clean=True,
        )

        # Stage4: Aquiring the rest of pages if available
        all_pages = loop_all_pages(page1, mc_api, args)

        # Stage5: Convert to a temp json file
        json_data = [json.dumps(item) for item in all_pages]

        with open(temp_file_path, "w") as json_file:
            json_file.write("\n".join(json_data))

        # Stage6: Upload json file to S3
        bucket_name = f"bb2-{env}-datalake-raw"
        abs_path = f"s3://{bucket_name}/{table_name}/{table_name}.json"

        logger.info("Uploading json file to " + abs_path)
        res = wr.s3.upload(temp_file_path, abs_path)
        os.remove(temp_file_path)
        logger.info("[Success]: Uploaded to " + abs_path)
        return res
    except Exception as e:
        logger.error(f"Job failed: {e}")
        raise


if __name__ == "__main__":
    main()
