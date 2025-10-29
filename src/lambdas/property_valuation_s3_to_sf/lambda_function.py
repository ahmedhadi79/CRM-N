import csv
import json
import logging
import os
import re
from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor
from io import StringIO

import boto3
import requests
from dateutil import parser

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def is_valid_email(email):
    """Check if the email is valid using a simple regex."""
    if email and email.lower() != "none":
        email_regex = r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)"
        return re.match(email_regex, email) is not None
    return False


def safe_int(value):
    try:
        # Convert only if value is a valid integer and not 'nan'
        if str(value).lower() != "nan" and value != "":
            return int(value)
        else:
            return None
    except ValueError:
        return None


def parse_currency(value):
    """Parses currency values, removes commas, symbols, and converts to float."""
    try:
        # Remove non-numeric characters except periods (for decimal values)
        cleaned_value = re.sub(r"[^\d.]", "", value)
        if not cleaned_value:
            return None
        return float(cleaned_value)
    except ValueError:
        logger.warning(f"Value '{value}' cannot be converted to float. Returning None.")
        return None


def parse_date(value):
    """Parses date fields and converts them to 'YYYY-MM-DD' format if possible."""
    if not value:
        return None

    try:
        parsed_date = parser.parse(value)
        # Adding a check for valid date ranges (adjust range as needed)
        if parsed_date.year > 2100 or parsed_date.year < 1900:
            logger.warning(f"Invalid date year: {parsed_date.year} in value: {value}")
            return None
        return parsed_date.strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        logger.warning(f"Invalid date format: {value}")
        return None


def get_secret(secret_name):
    """
    Retrieves a secret from AWS Secrets Manager
    """
    logger.info(f"Retrieving secret: {secret_name}")
    try:
        secretsmanager = boto3.client("secretsmanager")
        secret_value = secretsmanager.get_secret_value(SecretId=secret_name)
        return json.loads(secret_value["SecretString"])
    except Exception as e:
        logger.error(f"Exception occurred: {e}")
        return None


def get_auth_salesforce():
    """
    Issues the API request to Salesforce base url to get the access token for oauth2
    """
    logger.info("Fetching Salesforce auth token...")
    try:
        post_data = get_secret(os.environ["SALESFORCE_AUTH_DETAILS"])
        post_data["password"] += post_data.pop("security_token")
        TOKEN_URL = os.environ["TOKEN_URL"]

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        }
        auth_response = requests.post(TOKEN_URL, data=post_data, headers=headers)
        auth_response.raise_for_status()

        logger.info("Auth token retrieved successfully.")
        return auth_response.json()
    except Exception as e:
        logger.error(f"Error fetching Salesforce auth token: {e}")
        return None


def create_property_valuation(access_token, property_data, sf_instance_url, sf_version):
    """Create a Property Valuation record in Salesforce."""
    url = f"{sf_instance_url}/services/data/{sf_version}/sobjects/Property_Valuation__c"
    headers = {"Authorization": f"Bearer {access_token}"}

    try:
        response = requests.post(url, headers=headers, json=property_data)
        response.raise_for_status()
        return response.json().get("id")
    except requests.exceptions.HTTPError as err:
        logger.warning(f"Error creating Property Valuation: {err}")
        return None


def update_mortgage_case(
    access_token, case_id, property_valuation_id, sf_instance_url, sf_version
):
    """Update a Case with the Property Valuation ID."""
    url = f"{sf_instance_url}/services/data/{sf_version}/sobjects/Case/{case_id}"
    headers = {"Authorization": f"Bearer {access_token}"}
    data = {"Property_Valuation__c": property_valuation_id}

    try:
        response = requests.patch(url, headers=headers, json=data)
        response.raise_for_status()
        return response.status_code
    except requests.exceptions.HTTPError as err:
        logger.error(f"Error updating mortgage case {case_id}: {err}")
        return None


def read_csv_from_s3(bucket_name, file_key):
    """Fetches and reads the CSV file from S3."""
    s3_client = boto3.client("s3")
    try:
        s3_object = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        file_content = s3_object["Body"].read().decode("utf-8")
        return csv.DictReader(StringIO(file_content))
    except Exception as e:
        logger.error(f"Error reading CSV file from S3: {e}")
        return None


def process_csv_row(row, access_token):
    """Process a single CSV row."""
    sf_instance_url = os.environ["SALESFORCE_INSTANCE_URL"]
    sf_version = "v58.0"
    try:
        if not row.get("Case_ID__c"):
            logger.warning("Skipping row without Case_ID__c")
            return None

        property_data = {
            "Contact_Name__c": row.get("Contact_Name__c", ""),
            "Contact_Email__c": (
                row.get("Contact_Email__c", "")
                if is_valid_email(row.get("Contact_Email__c", ""))
                else ""
            ),
            "Contact_PhoneNumber__c": row.get("Contact_PhoneNumber__c", ""),
            "Property_Age__c": row.get("Property_Age__c", ""),
            "Expected_CompletionDate__c": row.get("Expected_CompletionDate__c", ""),
            "Property_Type__c": row.get("Property_Type__c", ""),
            "Bedroom_Count__c": safe_int(
                row.get("Bedroom_Count__c")
            ),  # (int(row["Bedroom_Count__c"]) if row.get("Bedroom_Count__c") else None),
            "Standard_Construction__c": row.get("Standard_Construction__c", ""),
            "Ownership_Type__c": row.get("Ownership_Type__c", ""),
            "Remaining_Lease_Years__c": row.get("Remaining_Lease_Years__c", ""),
            "Is_Ex_Local_Authority__c": row.get("Is_Ex_Local_Authority__c", ""),
            "Located_above_business__c": row.get("Located_above_business__c", ""),
            "Business_Nature__c": row.get("Business_Nature__c", ""),
            "Case_Number__c": row.get("Case_Number__c", ""),
            "Property_Address__c": row.get("Property_Address__c", ""),
            "Valuation_Date__c": parse_date(row.get("Valuation_Date__c", "")),
            "Market_Value__c": parse_currency(row.get("Market_Value__c", "")),
            "Insurance_Reinstatement_Estimate__c": parse_currency(
                row.get("Insurance_Reinstatement_Estimate__c", "")
            ),
            "Market_Rent_per_month__c": parse_currency(
                row.get("Market_Rent_per_month__c", "")
            ),
            "Essential_Repairs__c": row.get("Essential_Repairs__c", ""),
            "Flooding_risk_sea_or_river__c": row.get(
                "Flooding_risk_sea_or_river__c", ""
            ),
            "Flooding_Risk_surface_water__c": row.get(
                "Flooding_Risk_surface_water__c", ""
            ),
            "Invasive_Species__c": row.get("Invasive_Species__c", ""),
            "Overhead_Power_Lines__c": row.get("Overhead_Power_Lines__c", ""),
            "Tenure__c": row.get("Tenure__c", ""),
            "Terms_Lease_Details__c": row.get("Terms_Lease_Details__c", ""),
            "Service_Charge_per_annum__c": parse_currency(
                row.get("Service_Charge_per_annum__c", "")
            ),
            "Is_the_property_fit_for_immediate_occupa__c": row.get(
                "Is_the_property_fit_for_immediate_occupa__c", ""
            ),
            "Is_a_Reinspection_Required__c": (
                row.get("Is_a_Reinspection_Required__c", "") == "Yes"
            ),
            "Original_Building_construction_type__c": row.get(
                "Original_Building_construction_type__c", ""
            ),
            "External_Condition__c": row.get("External_Condition__c", ""),
            "Internal_Condition__c": row.get("Internal_Condition__c", ""),
            "Is_the_property_let__c": row.get("Is_the_property_let__c", ""),
            "Case_ID__c": row.get("Case_ID__c", ""),
        }

        case_id = row["Case_ID__c"]
        property_valuation_id = create_property_valuation(
            access_token, property_data, sf_instance_url, sf_version
        )

        if property_valuation_id:
            update_mortgage_case(
                access_token,
                case_id,
                property_valuation_id,
                sf_instance_url,
                sf_version,
            )

    except Exception as e:
        logger.error(f"Error processing row: {e} - Row data: {row}")
        return None


def process_csv_data(bucket_name, file_key):
    """Processes the CSV data by reading it from S3, creating property valuations, and updating cases."""
    csv_reader = read_csv_from_s3(bucket_name, file_key)

    if not csv_reader:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "CSV file is empty or unreadable."}),
        }

    # Step 1: Get Salesforce access token
    auth_response = get_auth_salesforce()
    if not auth_response:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Salesforce authentication failed."}),
        }

    access_token = auth_response["access_token"]

    # Step 2: Process rows concurrently for better performance
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(process_csv_row, row, access_token) for row in csv_reader
        ]
        for future in as_completed(futures):
            future.result()

    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Process completed successfully"}),
    }


# Main Lambda handler function
def lambda_handler(event, context):
    bucket_name = os.environ["S3_CURATED"]
    file_key = "salesforce-upload/property_valuation.csv"
    return process_csv_data(bucket_name, file_key)
