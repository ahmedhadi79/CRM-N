salesforce = {
    "salesforce_cases": {
        "reconcile_sql_path": "salesforce_cases_reconciliation.sql",
        "schema": {
            "createddate": "date",
            "type": "string",
            "open_closed": "string",
            "id_count_salesforce": "int",
            "id_count_athena": "int",
            "all_match": "boolean",
        },
    },
    "salesforce_surveyquestionresponse": {
        "reconcile_sql_path": "salesforce_sqr_reconciliation.sql",
        "schema": {
            "createddate": "date",
            "id_count_salesforce": "int",
            "id_count_athena": "int",
            "all_match": "boolean",
        },
    },
    "salesforce_surveytaker": {
        "reconcile_sql_path": "salesforce_st_reconciliation.sql",
        "schema": {
            "createddate": "date",
            "id_count_salesforce": "int",
            "id_count_athena": "int",
            "all_match": "boolean",
        },
    },
    "salesforce_survey": {
        "reconcile_sql_path": "salesforce_survey_reconciliation.sql",
        "schema": {
            "createddate": "date",
            "id_count_salesforce": "int",
            "id_count_athena": "int",
            "all_match": "boolean",
        },
    },
    "salesforce_accounthistory": {
        "reconcile_sql_path": "salesforce_accounthistory_reconciliation.sql",
        "schema": {
            "createddate": "date",
            "id_count_salesforce": "int",
            "id_count_athena": "int",
            "all_match": "boolean",
        },
    },
    "salesforce_account": {
        "reconcile_sql_path": "salesforce_account_reconciliation.sql",
        "schema": {
            "createddate": "date",
            "id_count_salesforce": "int",
            "id_count_athena": "int",
            "all_match": "boolean",
        },
    },
}
