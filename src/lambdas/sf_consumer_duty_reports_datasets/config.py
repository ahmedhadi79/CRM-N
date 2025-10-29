config = {
    "salesforce_complaints_cases_report": {
        "sql_path": "sf_complaints_cases_report.sql",
        "partition_cols": None,
        "catalog": {
            "case_record_type": {
                "type": "string",
                "comment": "The Salesforce case type name",
            },
            "case_number": {
                "type": "string",
                "comment": "The Salesforce case number",
            },
            "type": {
                "type": "string",
                "comment": "The Salesforce case type description",
            },
            "status": {
                "type": "string",
                "comment": "The Salesforce case status",
            },
            "complaint_status": {
                "type": "string",
                "comment": "The Salesforce case complaint status",
            },
            "response_status": {
                "type": "string",
                "comment": "The Salesforce case response status",
            },
            "redress_paid": {
                "type": "string",
                "comment": "The Salesforce case redress paid",
            },
            "redress_amount": {
                "type": "string",
                "comment": "The Salesforce case redress amount",
            },
            "complaint_root_cause": {
                "type": "string",
                "comment": "The Salesforce case complaint root cause",
            },
            "case_origin": {
                "type": "string",
                "comment": "The Salesforce case origin",
            },
            "case_owner": {
                "type": "string",
                "comment": "The Salesforce case owner",
            },
            "name": {
                "type": "string",
                "comment": "The Salesforce case contact",
            },
            "subject": {
                "type": "string",
                "comment": "The Salesforce case subject description",
            },
            "date_time_opened": {
                "type": "timestamp",
                "comment": "The Salesforce case creation date",
            },
            "date": {
                "type": "date",
                "comment": "date",
            },
        },
    },
    "salesforce_onboarding_survey_report": {
        "sql_path": "sf_onboarding_survey_report.sql",
        "partition_cols": None,
        "catalog": {
            "survey_response": {
                "type": "string",
                "comment": "The Salesforce survey response code",
            },
            "response_id": {
                "type": "string",
                "comment": "The Salesforce survey response id",
            },
            "question_type": {
                "type": "string",
                "comment": "The Salesforce survey response question type",
            },
            "name": {
                "type": "string",
                "comment": "The Salesforce survey response question name",
            },
            "response": {
                "type": "string",
                "comment": "The Salesforce survey response description",
            },
            "response_numeric": {
                "type": "string",
                "comment": "The Salesforce survey response numeric value",
            },
            "created_date": {
                "type": "timestamp",
                "comment": "The Salesforce case creation date",
            },
            "date": {
                "type": "date",
                "comment": "date",
            },
        },
    },
    "salesforce_general_enquiry_survey_report_CSAT": {
        "sql_path": "sf_general_enquiry_survey_report_CSAT.sql",
        "partition_cols": None,
        "catalog": {
            "survey_response": {
                "type": "string",
                "comment": "The Salesforce survey response code",
            },
            "response_id": {
                "type": "string",
                "comment": "The Salesforce survey response id",
            },
            "question_type": {
                "type": "string",
                "comment": "The Salesforce survey response question type",
            },
            "name": {
                "type": "string",
                "comment": "The Salesforce survey response question name",
            },
            "response": {
                "type": "string",
                "comment": "The Salesforce survey response description",
            },
            "response_numeric": {
                "type": "string",
                "comment": "The Salesforce survey response numeric value",
            },
            "created_date": {
                "type": "timestamp",
                "comment": "The Salesforce case creation date",
            },
            "date": {
                "type": "date",
                "comment": "date",
            },
        },
    },
    "salesforce_new_cssat_survey_report": {
        "sql_path": "sf_new_cssat_survey_report.sql",
        "partition_cols": None,
        "catalog": {
            "survey_response": {
                "type": "string",
                "comment": "The Salesforce survey response code",
            },
            "response_id": {
                "type": "string",
                "comment": "The Salesforce survey response id",
            },
            "question_type": {
                "type": "string",
                "comment": "The Salesforce survey response question type",
            },
            "name": {
                "type": "string",
                "comment": "The Salesforce survey response question name",
            },
            "response": {
                "type": "string",
                "comment": "The Salesforce survey response description",
            },
            "response_numeric": {
                "type": "string",
                "comment": "The Salesforce survey response numeric value",
            },
            "created_date": {
                "type": "timestamp",
                "comment": "The Salesforce case creation date",
            },
            "date": {
                "type": "date",
                "comment": "date",
            },
        },
    },
    "salesforce_complaint_cases_with_SLA_milestones_report": {
        "sql_path": "sf_complaint_cases_with_SLA_milestones_report.sql",
        "partition_cols": None,
        "catalog": {
            "case_number": {
                "type": "string",
                "comment": "The Salesforce case number",
            },
            "status": {
                "type": "string",
                "comment": "The Salesforce case status",
            },
            "priority": {
                "type": "string",
                "comment": "The Salesforce case type priority",
            },
            "case_origin": {
                "type": "string",
                "comment": "The Salesforce case origin",
            },
            "date_time_opened": {
                "type": "timestamp",
                "comment": "The Salesforce case creation date",
            },
            "milestone": {
                "type": "string",
                "comment": "The Salesforce case SLA milestone",
            },
            "target_date": {
                "type": "timestamp",
                "comment": "The Salesforce case SLA target date",
            },
            "completion_date": {
                "type": "timestamp",
                "comment": "The Salesforce case SAL completion date",
            },
            "case_record_type": {
                "type": "string",
                "comment": "The Salesforce case type name",
            },
            "date": {
                "type": "date",
                "comment": "date",
            },
        },
    },
    "salesforce_vulnerable_customers_report": {
        "sql_path": "sf_vulnerable_customers_report.sql",
        "partition_cols": None,
        "catalog": {
            "field": {
                "type": "string",
                "comment": "The Salesforce account history field",
            },
            "old_value": {
                "type": "string",
                "comment": "The Salesforce account history old value",
            },
            "new_value": {
                "type": "string",
                "comment": "The Salesforce account history value",
            },
            "account_id": {
                "type": "string",
                "comment": "The Salesforce account history account id",
            },
            "account_name": {
                "type": "string",
                "comment": "The Salesforce account name",
            },
            "created_date": {
                "type": "timestamp",
                "comment": "The Salesforce account history created date",
            },
            "date": {
                "type": "date",
                "comment": "date",
            },
        },
    },
}