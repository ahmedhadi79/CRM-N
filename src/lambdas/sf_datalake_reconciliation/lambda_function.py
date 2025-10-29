import json
import logging
import os
from typing import Dict

import awswrangler as wr
import boto3
import pandas as pd
import requests

import config

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_secret(secret_name):
    """
    Retrieves a secret from AWS Secrets Manager
    :param secret_name: The key to retrieve
    :return: The value of the secret
    """
    logger.info("Retrieving :  %s", secret_name)
    try:
        secretsmanager = boto3.client("secretsmanager")
        secret_value = secretsmanager.get_secret_value(SecretId=secret_name)
        return json.loads(secret_value["SecretString"])
    except Exception as e:
        logger.error("Exception occurred:  %s", e)
        print(e)
        return False


def get_auth_salesforce():
    """
    Issues the API request to Salesforce base url to get the access token for oauth2
    :return: The authentication response object
    """
    logger.info("Now retrieving the secret from AWS....")
    try:
        post_data = get_secret(os.environ["SALESFORCE_AUTH_DETAILS"])
    except Exception as e:
        logger.error(
            "Exception occurred in getting the secret from AWS Secrets:  %s", e
        )
        return None
    logger.info("Secret retrieved from AWS!")

    post_data["password"] = post_data["password"] + post_data["security_token"]
    post_data.pop("security_token")
    TOKEN_URL = os.environ["TOKEN_URL"]
    logger.info(f"Token URL: {TOKEN_URL}")
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
    }
    logger.info("Issuing request to get authentication details...")
    try:
        auth_response = requests.post(TOKEN_URL, data=post_data, headers=headers)
    except Exception as e:
        logger.error("Exception occurred in getting the token:  %s", e)
        return None

    if auth_response.status_code != 200:
        logger.error("Exception occurred in getting the token!")
        logger.error(auth_response.json())
        return None

    logger.info("Auth details retrieved!")

    return auth_response.json()


def get_salesforce_case_data_df():
    """
    Issues the API request to Salesforce endpoint to get case data
    :return: Pandas dataframe containing the salesforce case data
    """
    auth_response = get_auth_salesforce()
    instance_url = auth_response["instance_url"]
    token = auth_response["access_token"]

    api_version = os.environ["SALESFORCE_API_VERSION"]

    # define vars
    cases_url = (
        f"/services/data/{api_version}/query/?q=SELECT+Id,+Type,+Status,+Customer_Reference_Id__c"
        + ",+priority,+description,+CreatedDate,+LastModifiedDate,+ClosedDate,+OwnerId,+Origin,+ContactId,+CaseNumber"
        + ",+case_record_type_name__c,+Case_RT_Dev_Name__c,+Subject,+complaint_status__c,+response_status__c"
        + ",+redress_paid__c,+redress_amount__c,+Complaint_Root_Cause__c+FROM+Case"
    )
    headers = {"Authorization": "Bearer {}".format(token)}
    next_url = None
    done = False
    cases_json = None
    final_df = pd.DataFrame()

    # loop until no more results from the salesforce api and concate to a pandas dataframe
    while not done:
        if next_url:
            next_url = cases_json["nextRecordsUrl"]
            full_request_url = instance_url + next_url
        else:
            full_request_url = instance_url + cases_url
        try:
            cases_response = requests.get(full_request_url, headers=headers)
        except Exception as e:
            logger.error("Exception occurred:  %s", e)
            return None
        cases_json = cases_response.json()
        temp_df = pd.DataFrame(cases_json["records"])
        final_df = pd.concat([final_df, temp_df], ignore_index=True)
        next_url = (
            cases_json["nextRecordsUrl"] if "nextRecordsUrl" in cases_json else None
        )
        done = next_url is None

    # discard system columns not needed and fix columns
    final_df.drop("attributes", axis=1, inplace=True)
    final_df["Description"] = final_df["Description"].str.replace("\n", " ")
    final_df["Type"] = final_df["Type"].str.replace("\n", " ")
    final_df["Description"] = final_df["Description"].str.replace("\r", " ")
    final_df["Type"] = final_df["Type"].str.replace("\r", " ")

    return final_df


def get_salesforce_data_df(sf_recon_table):
    """
    Issues the API request to Salesforce endpoint to get surveyquestionresponse data
    :return: Pandas dataframe containing the salesforce surveyquestionresponse data
    """
    auth_response = get_auth_salesforce()
    instance_url = auth_response["instance_url"]
    token = auth_response["access_token"]

    api_version = os.environ["SALESFORCE_API_VERSION"]
    if sf_recon_table == "salesforce_surveyquestionresponse":
        sf_url = (
            f"/services/data/{api_version}/query/?q=SELECT+Id,+Name,+Question_Type__c,+Response__c"
            + ",+Response_Numeric__c,+Survey_Question__c,+SurveyTaker__c,+Survey__c"
            + ",+CreatedDate+FROM+SurveyQuestionResponse__c"
        )
    elif sf_recon_table == "salesforce_surveytaker":
        sf_url = (
            f"/services/data/{api_version}/query/?q=SELECT+Id,+Name"
            + ",+Survey__c"
            + ",+CreatedDate+FROM+SurveyTaker__c"
        )
    elif sf_recon_table == "salesforce_accounthistory":
        sf_url = (
            f"/services/data/{api_version}/query/?q=SELECT+Field,+OldValue,+NewValue,+AccountId"
            + ",+CreatedDate+FROM+AccountHistory"
        )
    elif sf_recon_table == "salesforce_account":
        sf_url = (
            f"/services/data/{api_version}/query/?q=SELECT+Id,+Name"
            + ",+CreatedDate+FROM+Account"
        )
    else:
        sf_url = (
            f"/services/data/{api_version}/query/?q=SELECT+Id,+Name"
            + ",+CreatedDate+FROM+Survey__c"
        )

    headers = {"Authorization": "Bearer {}".format(token)}
    next_url = None
    done = False
    sf_json = None
    final_df = pd.DataFrame()

    # loop until no more results from the salesforce api and concate to a pandas dataframe
    while not done:
        if next_url:
            next_url = sf_json["nextRecordsUrl"]
            full_request_url = instance_url + next_url
        else:
            full_request_url = instance_url + sf_url
        try:
            sf_response = requests.get(full_request_url, headers=headers)
        except Exception as e:
            logger.error("Exception occurred:  %s", e)
            return None
        sf_json = sf_response.json()
        temp_df = pd.DataFrame(sf_json["records"])
        final_df = pd.concat([final_df, temp_df], ignore_index=True)
        next_url = sf_json["nextRecordsUrl"] if "nextRecordsUrl" in sf_json else None
        done = next_url is None

    # discard system columns not needed and fix columns
    final_df.drop("attributes", axis=1, inplace=True)

    return final_df


def read_athena(sql_path: str, input_database: str) -> pd.DataFrame:
    with open(sql_path, "r") as sql_file:
        sql = sql_file.read()

    logger.info("Reading from Athena... ")

    df = wr.athena.read_sql_query(
        sql=sql,
        database=input_database,
        workgroup="datalake_workgroup",
        ctas_approach=False,
    )

    return df


def construct_count_df(salesforce_df: pd.DataFrame, athena_df: pd.DataFrame):
    salesforce_df["Type"] = salesforce_df["Type"].fillna(
        salesforce_df["case_record_type_name__c"]
    )
    salesforce_df["createddate"] = pd.to_datetime(salesforce_df["CreatedDate"]).dt.date
    salesforce_df = salesforce_df.rename(columns={"Id": "id", "Type": "type"})

    athena_df["createddate"] = pd.to_datetime(athena_df["createddate"]).dt.date

    athena_grouped = (
        athena_df.groupby(["createddate", "type", "open_closed"])
        .count()["id"]
        .reset_index()
        .rename(columns={"id": "id_count"})
    )

    salesforce_df["open_closed"] = [
        "CLOSED" if x else "OPEN" for x in salesforce_df["ClosedDate"]
    ]
    salesforce_grouped = (
        salesforce_df.groupby(["createddate", "type", "open_closed"])
        .count()["id"]
        .reset_index()
        .rename(columns={"id": "id_count"})
    )

    merge_df = pd.merge(
        salesforce_grouped,
        athena_grouped,
        on=["createddate", "type", "open_closed"],
        how="outer",
        suffixes=["_salesforce", "_athena"],
    )

    merge_df["all_match"] = all(
        merge_df["id_count_salesforce"] == merge_df["id_count_athena"]
    )

    return merge_df


def count_df(salesforce_df: pd.DataFrame, athena_df: pd.DataFrame):
    salesforce_df["createddate"] = pd.to_datetime(salesforce_df["CreatedDate"]).dt.date
    salesforce_df = salesforce_df.rename(columns={"Id": "id"})

    athena_df["createddate"] = pd.to_datetime(athena_df["createddate"]).dt.date

    athena_grouped = (
        athena_df.groupby(["createddate"])
        .count()["id"]
        .reset_index()
        .rename(columns={"id": "id_count"})
    )

    salesforce_grouped = (
        salesforce_df.groupby(["createddate"])
        .count()["id"]
        .reset_index()
        .rename(columns={"id": "id_count"})
    )

    merge_df = pd.merge(
        salesforce_grouped,
        athena_grouped,
        on=["createddate"],
        how="outer",
        suffixes=["_salesforce", "_athena"],
    )

    merge_df["all_match"] = all(
        merge_df["id_count_salesforce"] == merge_df["id_count_athena"]
    )

    return merge_df


def count_account_history_df(salesforce_df: pd.DataFrame, athena_df: pd.DataFrame):
    salesforce_df["createddate"] = pd.to_datetime(salesforce_df["CreatedDate"]).dt.date
    salesforce_df = salesforce_df.rename(columns={"AccountId": "id"})

    athena_df["createddate"] = pd.to_datetime(athena_df["createddate"]).dt.date

    athena_grouped = (
        athena_df.groupby(["createddate"])
        .count()["id"]
        .reset_index()
        .rename(columns={"id": "id_count"})
    )

    salesforce_grouped = (
        salesforce_df.groupby(["createddate"])
        .count()["id"]
        .reset_index()
        .rename(columns={"id": "id_count"})
    )

    merge_df = pd.merge(
        salesforce_grouped,
        athena_grouped,
        on=["createddate"],
        how="outer",
        suffixes=["_salesforce", "_athena"],
    )

    merge_df["all_match"] = all(
        merge_df["id_count_salesforce"] == merge_df["id_count_athena"]
    )

    return merge_df


def create_database_if_not_exists(database_name: str) -> Dict:
    """
    Creates a database in Athena with the name provided if it doesn't already exist
    :param database_name: The name of the database
    :type database_name: str
    :return: The response
    :rtype: dict
    """

    if database_name not in wr.catalog.databases().values:
        res = wr.catalog.create_database(database_name)

        return res


def write_to_s3(
    output_df: pd.DataFrame,
    athena_table: str,
    database_name: str,
    schema: Dict[str, str],
    s3_bucket: str = None,
    mode: str = "append",
) -> dict:
    """
    Writes the DataFrame to S3 and Athena using AWS Wrangler
    :param output_df: The Dataframe to write out to S3
    :type output_df: pd.DataFrame
    :param athena_table: The table in Athena to write to
    :type athena_table: str
    :param database_name: The database in Athena to write to
    :type database_name: str
    :param s3_bucket: The name of the bucket in S3, defaults to None
    :type s3_bucket: str, optional
    :return: The response
    :rtype: dict
    """

    if s3_bucket is None:
        s3_bucket = os.environ["S3_RECON"]

    logger.info(f"Uploading to S3 bucket: {s3_bucket}")
    logger.info(f"Pandas DataFrame Shape: {output_df.shape}")
    path = f"s3://{s3_bucket}/{athena_table}/"
    logger.info("Uploading to S3 location:  %s", path)

    create_database_if_not_exists(database_name)

    try:
        res = wr.s3.to_csv(
            df=output_df,
            path=path,
            index=False,
            dataset=True,
            database=database_name,
            table=athena_table,
            mode=mode,
            schema_evolution="true",
            dtype=schema,
        )

        return res

    except Exception as e:
        logger.error("Failed uploading to S3 location:  %s", path)
        logger.error("Exception occurred:  %s", e)

        return e


def lambda_handler(event, context):
    """[summary]

    :param event: [description]
    :type event: [type]
    :param context: [description]
    :type context: [type]
    :return: [description]
    :rtype: [type]
    """

    logger.info("Getting salesforce case data...")

    for sf_recon_table in config.salesforce.keys():
        sql_path = config.salesforce[sf_recon_table]["reconcile_sql_path"]
        athena_df = read_athena(sql_path, "datalake_raw")
        if sf_recon_table == "salesforce_cases":
            sf_df = get_salesforce_case_data_df()
            merge_df = construct_count_df(sf_df, athena_df)
        else:
            sf_df = get_salesforce_data_df(sf_recon_table)
            if sf_recon_table == "salesforce_accounthistory":
                merge_df = count_account_history_df(sf_df, athena_df)
            else:
                merge_df = count_df(sf_df, athena_df)

        logger.info(f"Now initiaiting write to s3 routine {sf_recon_table}")
        write_to_s3(
            merge_df,
            athena_table=f"{sf_recon_table}",
            database_name="datalake_reconciliation",
            schema=config.salesforce[sf_recon_table]["schema"],
            mode="overwrite",
        )
