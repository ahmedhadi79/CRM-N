import logging
import sys
import time

from api_client import APIClient
from awsglue.utils import getResolvedOptions
from custom_functions import get_salesforce_data
from custom_functions import get_salesforce_df
from custom_functions import logger
from custom_functions import raw_load_to_s3
from data_catalog import column_comments
from data_catalog import schemas
from salesforce_queries import config

table_name = "salesforce_emailmessage"


class CustomFormatter(logging.Formatter):
    grey = "\x1b[38;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    green = "\x1b[32;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    format = "%(asctime)s - %(levelname)s - %(message)s"

    FORMATS = {
        logging.DEBUG: grey + format + reset,
        logging.INFO: green + format + reset,
        logging.WARNING: yellow + format + reset,
        logging.ERROR: red + format + reset,
        logging.CRITICAL: bold_red + format + reset,
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


def _get_logger(module_name):
    try:
        logger = logging.getLogger(module_name)
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setFormatter(CustomFormatter())
        logger.addHandler(handler)
        logger.info("Logger initialized successfully!")
        return logger
    except Exception as e:
        sys.exit(e)


logger = _get_logger("sf-emailmessage-tos3raw-backfill")


def main():
    begin = time.time()
    args = getResolvedOptions(
        sys.argv,
        [
            "ENV",
            "TOKEN_URL",
            "SALESFORCE_AUTH_DETAILS",
            "SALESFORCE_API_VERSION",
        ],
    )
    env = args["ENV"]
    token_url = args["TOKEN_URL"]
    auth_details = args["SALESFORCE_AUTH_DETAILS"]
    api_version = args["SALESFORCE_API_VERSION"]

    salesforce_api = APIClient(
        login_url=token_url,
        auth=auth_details,
        secrets_manager=True,
    )
    sf_version = api_version
    data = salesforce_api.get(
        endpoint=f"services/data/{sf_version}/query/",
        filter_objects=["done", "nextRecordsUrl", "records"],
        clean=True,
        query=config["queries"][f"{table_name}_backfill"],
    )
    data_list = get_salesforce_data(data, salesforce_api)
    if data_list == [[]]:
        logger.info(f"The data list is empty: {data_list}")
    else:
        df = get_salesforce_df(data_list)
        res = raw_load_to_s3(
            ingested_df=df,
            table_name=table_name,
            column_comments=column_comments,
            schemas=schemas,
            env=env,
            file_type="csv",
            mode="overwrite_partitions",
        )
        if res:
            logger.info("Finished loading backfill")
        else:
            logger.info("Failed to process the backfill data ")
    end = time.time()
    logger.info(
        f"Total minutes taken for this Lambda to run: {float((end - begin) / 60):.2f}"
    )


if __name__ == "__main__":
    main()
