SELECT
    id,
    name,
    question_type__c,
    response__c,
    response_numeric__c,
    survey_question__c,
    surveytaker__c,
    survey__c,
    createddate,
    DAY(
        DATE_PARSE(SUBSTR(createddate, 1, 10), '%Y-%m-%d')
    ) AS day,
    MONTH(
        DATE_PARSE(SUBSTR(createddate, 1, 10), '%Y-%m-%d')
    ) AS month,
    QUARTER(
        DATE_PARSE(SUBSTR(createddate, 1, 10), '%Y-%m-%d')
    ) AS quarter,
    YEAR(
        DATE_PARSE(SUBSTR(createddate, 1, 10), '%Y-%m-%d')
    ) AS year
FROM
    datalake_raw.salesforce_surveyquestionresponse;
