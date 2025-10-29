# Project Overview
This project involves extracting data from Salesforce, processing it, and then storing it in AWS S3. The code is designed to be executed within an AWS Lambda function. The key components of the project include:

	•	Importing necessary libraries and modules
	•	Fetching data from the Salesforce API
	•	Flattening data with flatten_json
    •   Processing the data into a pandas DataFrame
	•	Saving the processed data to AWS S3 bucket ==> bb2-prod-datalake-raw/salesforce_psd_form_c

# Running

	•	The cron job for this lambda "cron(30 00 * * ? *)"
	•	This Lambda was created to send API request to Salesforce with this query:
			- "salesforce_psd_form_c": "q=select Administrator_FRN__c,First_borrower_DOB__c,Mortgage_acct_open_date__c,Original_mortgage_term_months__c, Original_transaction_reference__c,Date_of_balance__c,Original_size_of_loan__c,Original_provider_FRN__c,FRN_of_beneficial_owner__c,  Name_of_legal_owner__c,Current_balance_outstanding__c,Current_expected_monthly_payment__c,Current_gross_rate_of_interest__c,  Current_interest_rate_type__c,Current_rate_incentivised__c,Date_incentivised_rate_ends__c,Remaining_term_months__c,Months_past_maturity__c,
            Current_payment_shortfall__c,Arrears_start_date__c,Formal_arrangement_for_shortfall__c,Date_of_formal_arrangement__c,Capitalisation_of_payment_shortfall__c, Capitalisation_date__c,Date_of_interest_only_switch__c,Payments_suspended__c,Date_payments_suspended__c,Date_of_reduced_payment__c,Term_extension__c,
            Date_of_term_extension__c,Date_of_other_forbearance__c,Date_litigation_action_started__c,Date_of_possession__c,Sale_value_achieved_for_repossessions__c, Case__c,Loan_purchased__c,Is_the_beneficial_owner_a_regulated_firm__c,Is_the_legal_owner_a_regulated_firm__c,Repayment_type__c,Reason_for_closure__c,
            Temporary_switch_to_interest_only__c,Reduced_payment__c,Other_forbearance__c,Possession_order__c,Second_charge_mortgage__c,Is_the_account_part_of_a_closed_ Value_of_linked_accounts__c from PSD_Form__c"
	•	The response comning with json data
	•	The json data has been flattened with flatten_json lib and became python data list.
	•	The python data list transformed to data frame with Pandas lib after processing on it.
	•	Using raw_load_to_s3 function to load the csv file to bb2-datalake-raw and creating a table "salesforce_psd_form_c".


# Prerequisites

Before running the code, ensure you have the following prerequisites:

	- Python Libraries: The necessary Python libraries include os, pandas, flatten_json, datetime, salesforce_queries, api_client, and custom_functions.

	- Environment Variables: The following environment variables should be set:

	    •	TOKEN_URL: URL for Salesforce token
	    •	SALESFORCE_AUTH_DETAILS: Authentication details for Salesforce
	    •	SALESFORCE_API_VERSION: Salesforce API version
	    •	ENV: Environment name (e.g., dev, prod)

# File Structure
    .
    ├── main.py                   # Main script containing the Lambda handler
    ├── api_client.py             # Contains the APIClient class for handling API requests
    ├── custom_functions.py       # Contains custom utility functions
    ├── salesforce_queries.py     # Contains Salesforce query configurations
    ├── data_catalog.py           # Contains column comments and schemas
    └── requirements.txt          # List of required Python packages


# Digram

![Alt text](salesforce_psd_form_c.png)
