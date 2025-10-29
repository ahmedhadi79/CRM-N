config = {
    "salesforce_outboundcall_mi_report": {
        "sql_path": "outboundcall_mi_report.sql",
        "partition_cols": None,
        "catalog": {
            "id": {
                "type": "string",
                "comment": "The Salesforce contact ID",
            },
            "customer_cohort__c": {
                "type": "string",
                "comment": "The Salesforce customer description",
            },
            "prior_customer_cohort__c": {
                "type": "string",
                "comment": "The Salesforce prior customer",
            },
            "latest_cohort_change__c": {
                "type": "string",
                "comment": "The Salesforce latest change status",
            },
            "customer_intent__c": {
                "type": "string",
                "comment": "The Salesforce customer intent",
            },
            "intent_reason__c": {
                "type": "string",
                "comment": "The Salesforce customer reason",
            },
            "last_call_date__c": {
                "type": "string",
                "comment": "The contact last call date",
            },
            "ocp_call_1__c": {
                "type": "boolean",
                "comment": "The contact call info",
            },
            "ocp_call_1_result__c": {
                "type": "string",
                "comment": "The contact call result",
            },
            "ocp_call_1_date__c": {
                "type": "string",
                "comment": "The contact call date",
            },
            "ocp_call_2__c": {
                "type": "boolean",
                "comment": "The contact call info",
            },
            "ocp_call_2_result__c": {
                "type": "string",
                "comment": "The contact call result",
            },
            "ocp_call_2_date__c": {
                "type": "timestamp",
                "comment": "The contact call date",
            },
            "ocp_call_3__c": {
                "type": "boolean",
                "comment": "The contact call info",
            },
            "ocp_call_3_result__c": {
                "type": "string",
                "comment": "The contact call result",
            },
            "ocp_call_3_date__c": {
                "type": "string",
                "comment": "The contact call date",
            },
            "final_call__c": {
                "type": "boolean",
                "comment": "The contact call final info",
            },
            "final_call_attempt_date__c": {
                "type": "string",
                "comment": "The contact call date",
            },
            "final_call_projected_date__c": {
                "type": "string",
                "comment": "The contact call projected date",
            },
            "final_call_result__c": {
                "type": "string",
                "comment": "TThe contact call final call date",
            },
            "pending_final_call__c": {
                "type": "boolean",
                "comment": "The contact call status",
            },
        },
    }
}
