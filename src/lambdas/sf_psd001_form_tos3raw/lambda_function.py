import os

# Case1: Execution inside AWS Lambda
if "AWS_LAMBDA_FUNCTION_NAME" in os.environ:
    from data_catalog import column_comments, schemas
    import salesforce_queries
    from api_client import APIClient
    from custom_functions import (
        logger,
        raw_load_to_s3,
        get_salesforce_data,
        get_salesforce_df,
    )

# Case2: Local or test execution
else:
    from .data_catalog import column_comments, schemas
    from src.common import salesforce_queries
    from src.common.api_client import APIClient
    from src.common.custom_functions import (
        logger,
        raw_load_to_s3,
        get_salesforce_data,
        get_salesforce_df,
    )

table_name = "salesforce_psd001_form_c"

logger.info("Saving results..")


def lambda_handler(event, context):
    logger.info("Initializing API handler..")
    salesforce_api = APIClient(
        login_url=os.environ["TOKEN_URL"],
        auth=os.environ["SALESFORCE_AUTH_DETAILS"],
        secrets_manager=True,
    )
    sf_version = os.environ["SALESFORCE_API_VERSION"]
    data = salesforce_api.get(
        endpoint=f"services/data/{sf_version}/query/",
        filter_objects=["done", "nextRecordsUrl", "records"],
        clean=True,
        query=salesforce_queries.config["queries"][table_name],
    )
    data_list = get_salesforce_data(data, salesforce_api)
    df = get_salesforce_df(data_list)
    # Drop unwanted columns if they exist
    df.drop(columns=["Case__r_attributes_type", "Case__r_attributes_url"], errors="ignore", inplace=True)
    # Rename the Salesforce nested field to flat schema field
    if "Case__r_Property_Finance_Type__c" in df.columns:
        df["property_finance_type__c"] = df["Case__r_Property_Finance_Type__c"]
        df.drop(columns=["Case__r_Property_Finance_Type__c"], inplace=True)
    else:
        logger.warning("Column 'Case__r_Property_Finance_Type__c' not found in DataFrame.")
        df["property_finance_type__c"] = None

    res = raw_load_to_s3(
        ingested_df=df,
        table_name=table_name,
        column_comments=column_comments,
        schemas=schemas,
        env=os.environ["ENV"],
        file_type="parquet",
        mode="overwrite",
    )

    return res
