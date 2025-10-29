column_comments = {
    "mc_data_extensions_list": {
        "id": "Unique identifier for each record in the data extension.",
        "key": "Unique identifier for the data extension.",
        "dataExtensionName": "Name of the data extension.",
        "isSendable": "Indicates if the data extension is sendable (True/False).",
        "isTestable": "Indicates if the data extension is testable (True/False).",
        "dataExtensionTemplateId": "Identifier for the template linked with the data extension.",
        "isPublic": "Indicates if the data extension is public (True/False).",
        "isPlatformObject": "Indicates if the data extension is a platform object (True/False).",
        "description": "Description of the data extension.",
        "sendableDataExtensionField": "Field used for sending emails.",
        "sendableSubscriberField": "Field used for identifying subscribers.",
        "date": "Date associated with the record extraction date.",
        "timestamp_extracted": "Timestamp of when the data was extracted.",
    }
}

schemas = {
    "mc_data_extensions_list": {
        "id": "string",
        "key": "string",
        "dataExtensionName": "string",
        "isSendable": "boolean",
        "isTestable": "boolean",
        "dataExtensionTemplateId": "string",
        "isPublic": "boolean",
        "isPlatformObject": "boolean",
        "description": "string",
        "sendableDataExtensionField": "string",
        "sendableSubscriberField": "string",
        "date": "date",
        "timestamp_extracted": "timestamp",
    }
}
