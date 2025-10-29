column_comments = {
    "mc_campaigns": {
        "createdDate": "The date and time when the campaign was created.",
        "modifiedDate": "The date and time when the campaign was last modified.",
        "id": "The unique identifier for the campaign (integer).",
        "name": "The name of the campaign (string).",
        "description": "A brief description of the campaign (string).",
        "campaignCode": "The code associated with the campaign (string).",
        "color": "The color associated with the campaign (string).",
        "favorite": "Indicates whether the campaign is marked as a favorite (boolean).",
        "date": "The date associated with the campaign (date).",
        "timestamp_extracted": "The timestamp when data was extracted for the campaign (timestamp).",
    }
}

schemas = {
    "mc_campaigns": {
        "createdDate": "timestamp",
        "modifiedDate": "timestamp",
        "id": "int",
        "name": "string",
        "description": "string",
        "campaignCode": "string",
        "color": "string",
        "favorite": "boolean",
        "date": "date",
        "timestamp_extracted": "timestamp",
    }
}
