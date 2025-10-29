import json
import logging
import sys
import time
import xml.etree.ElementTree as ET
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

import boto3
import requests


def setup_logger(
    name: Optional[str] = None,
    level: int = logging.INFO,
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    filename: Optional[str] = None,
) -> logging.Logger:
    """
    Sets up a logger with the specified configuration.

    Parameters:
    - name (Optional[str]): Name of the logger. If None, the root logger is used.
    - level (int): Logging level (e.g., logging.INFO, logging.DEBUG).
    - format (str): Log message format.
    - filename (Optional[str]): If specified, logs will be written to this file. Otherwise, logs are written to stdout.

    Returns:
    - logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if filename:
        handler = logging.FileHandler(filename)
    else:
        handler = logging.StreamHandler(sys.stdout)

    handler.setLevel(level)
    formatter = logging.Formatter(format)
    handler.setFormatter(formatter)

    # To avoid duplicate handlers being added
    if not logger.hasHandlers():
        logger.addHandler(handler)

    return logger


logger = setup_logger("soap_client")


class SOAPClient:
    def __init__(
        self,
        login_url: str,
        auth: Union[str, dict],
        secrets_manager: bool = False,
        boto3_session: boto3.Session = None,
    ):
        """
        Initialize the APIClient with necessary parameters.

        :param login_url: The URL to login
        :param auth: Authentication information, either a secret name or a dictionary
        :param secrets_manager: Whether to use AWS Secrets Manager to retrieve the secret
        :param boto3_session: boto3 session for AWS interactions
        """
        self.login_url = login_url
        self._get_secret(auth, secrets_manager, boto3_session)
        self.logged_in = False

    def _get_secret(
        self, secret_name: str, secrets_manager: bool, boto3_session: boto3.Session
    ) -> Union[str, Dict[str, Any]]:
        """
        Retrieve a secret from AWS Secrets Manager.

        :param secret_name: The key to retrieve
        :param secrets_manager: Whether to use AWS Secrets Manager
        :param boto3_session: boto3 session for AWS interactions
        :return: The secret value
        """
        if secrets_manager:
            logger.info("Retrieving: %s", secret_name)
            secretsmanager = (
                boto3_session.client("secretsmanager")
                if boto3_session
                else boto3.client("secretsmanager")
            )
            secret_value = secretsmanager.get_secret_value(SecretId=secret_name)[
                "SecretString"
            ]
        else:
            secret_value = secret_name

        logger.info("Auth Header: OAuth, logging in...")

        if isinstance(secret_value, str):
            secret_value = json.loads(secret_value)

        self._login(secret_value)

    def _login(self, secret_value: Dict[str, Any]):
        """
        Login to the API and retrieve access token.

        :param secret_value: The secret value containing authentication information
        """
        max_retries = 3
        for attempt in range(max_retries):
            response = requests.post(self.login_url, data=secret_value)
            if response.status_code == 200:
                login_payload = self._parse_json_response(response)
                self.__soap_instance_url = login_payload["soap_instance_url"]
                self.__access_token = login_payload["access_token"]
                self.logged_in = True
                logger.info("Login success!")
                break
            else:
                logger.info(
                    f"Login attempt {attempt + 1} failed with status code {response.status_code}, retrying..."
                )
                if attempt < max_retries - 1:
                    time.sleep(1)
        else:
            error_message = f"Failed to authenticate after {max_retries} attempts"
            logger.error(error_message)
            raise Exception(error_message)

    @staticmethod
    def _parse_json_response(
        response: requests.Response,
    ) -> Union[Dict[str, Any], requests.Response]:
        """
        Parse a JSON response from the API.

        :param response: The response object
        :return: Parsed JSON data or raw response
        """
        if "application/json" not in response.headers.get("Content-Type", ""):
            logger.error("Response is not application/json, returning raw response")
            return response

        try:
            return response.json()
        except ValueError:
            logger.error("Could not convert response to JSON, returning raw response")
            return response

    @staticmethod
    def _parse_xml_response(
        response: requests.Response, properties: List[str]
    ) -> Union[Dict[str, Any], requests.Response]:
        """
        Parse an XML response from the API.

        :param response: The response object
        :param properties: The properties to extract from the XML
        :return: Parsed XML data or raw response
        """
        if "application/soap+xml" not in response.headers.get("Content-Type", ""):
            logger.error("Response is not application/xml, returning raw response")
            return response

        try:
            root = ET.fromstring(response.text)
            ns = {
                "soap": "http://www.w3.org/2003/05/soap-envelope",
                "xsi": "http://www.w3.org/2001/XMLSchema-instance",
                "": "http://exacttarget.com/wsdl/partnerAPI",
            }

            retrieve_response_msg = root.find(".//soap:Body//RetrieveResponseMsg", ns)
            if retrieve_response_msg is None:
                logger.error("RetrieveResponseMsg not found in the response")
                return response

            data = {
                "OverallStatus": retrieve_response_msg.find("OverallStatus", ns).text,
                "RequestID": retrieve_response_msg.find("RequestID", ns).text,
                "Results": [],
            }

            for result in retrieve_response_msg.findall("Results", ns):
                result_data = {
                    prop: (
                        result.find(prop, ns).text
                        if result.find(prop, ns) is not None
                        else None
                    )
                    for prop in properties
                }
                data["Results"].append(result_data)

            return data

        except ET.ParseError:
            logger.error("Could not parse XML response, returning raw response")
        except Exception as e:
            logger.error(f"Unexpected error: {e}, returning raw response")

        return response

    def _generate_soap_body(
        self,
        object_type: str,
        properties: List[str] = None,
        filter_operator: str = None,
        filter_property: str = None,
        filter_values: List[str] = None,
        continue_request: bool = False,
    ) -> str:
        """
        Generate the SOAP request body.

        :param object_type: The object type to retrieve
        :param properties: The properties to retrieve
        :param filter_operator: The filter operator to use
        :param filter_property: The property to filter on
        :param filter_values: The values to filter with
        :param continue_request: Whether to continue a previous request
        :return: The SOAP request body
        """

        object_xml = (
            f"<ContinueRequest>{object_type}</ContinueRequest>"
            if continue_request
            else f"<ObjectType>{object_type}</ObjectType>"
        )

        properties_xml = (
            "".join([f"<Properties>{prop}</Properties>" for prop in properties])
            if properties
            else ""
        )
        filter_values_xml = (
            "".join([f"<Value>{value}</Value>" for value in filter_values])
            if filter_values
            else ""
        )
        filter_xml = (
            f"""
        <Filter xsi:type="SimpleFilterPart" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
            <Property>{filter_property}</Property>
            <SimpleOperator>{filter_operator}</SimpleOperator>
            {filter_values_xml}
        </Filter>
        """
            if filter_operator and filter_property and filter_values
            else ""
        )

        return f"""
        <s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope" xmlns:a="http://schemas.xmlsoap.org/ws/2004/08/addressing" xmlns:u="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
            <s:Header>
                <a:Action s:mustUnderstand="1">Retrieve</a:Action>
                <a:To s:mustUnderstand="1">{self.__soap_instance_url}Service.asmx</a:To>
                <fueloauth xmlns="http://exacttarget.com">{self.__access_token}</fueloauth>
            </s:Header>
            <s:Body xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
                <RetrieveRequestMsg xmlns="http://exacttarget.com/wsdl/partnerAPI">
                    <RetrieveRequest>
                        {object_xml}
                        {properties_xml}
                        {filter_xml}
                    </RetrieveRequest>
                </RetrieveRequestMsg>
            </s:Body>
        </s:Envelope>
        """

    def request(
        self,
        object_type: str,
        properties: List[str],
        filter_operator: str = None,
        filter_property: str = None,
        filter_values: List[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Make a request to the API and retrieve data.

        :param object_type: The object type to retrieve
        :param properties: The properties to retrieve
        :param filter_operator: The filter operator to use https://developer.salesforce.com/docs/marketing/marketing-cloud/guide/simpleoperators.html
        :param filter_property: The property to filter on
        :param filter_values: The values to filter with
        :return: The retrieved data
        """

        endpoint = f"{self.__soap_instance_url}Service.asmx"
        headers = {"Content-Type": "text/xml; charset=utf-8"}

        logger.info("Fetching page 1")
        body = self._generate_soap_body(
            object_type=object_type,
            properties=properties,
            filter_operator=filter_operator,
            filter_property=filter_property,
            filter_values=filter_values,
        )
        response_1_xml = requests.request("POST", endpoint, headers=headers, data=body)
        response_1_json = self._parse_xml_response(response_1_xml, properties)

        return self._fetch_all_pages(response_1_json, endpoint, headers, properties)

    def _fetch_all_pages(
        self,
        first_payload: Dict[str, Any],
        endpoint: str,
        headers: Dict[str, str],
        properties: List[str],
    ) -> List[Dict[str, Any]]:
        """
        Fetch all pages of data if pagination is required.

        :param first_payload: The initial payload
        :param endpoint: The API endpoint
        :param headers: The request headers
        :param properties: The properties to retrieve
        :return: All retrieved data
        """
        data = []
        request_json = first_payload
        page_num = 1

        while True:
            data.extend(request_json["Results"])

            if "MoreDataAvailable" in request_json["OverallStatus"]:
                page_num += 1
                logger.info(f"Fetching page {page_num}")
                request_body = self._generate_soap_body(
                    object_type=request_json["RequestID"], continue_request=True
                )
                request_xml = requests.request(
                    "POST", endpoint, headers=headers, data=request_body
                )
                request_json = self._parse_xml_response(request_xml, properties)
            else:
                break

        return data
