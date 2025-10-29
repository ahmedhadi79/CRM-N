select id,
    name,
    createddate,
    DAY(createddate) as day,
    MONTH(createddate) as month,
    QUARTER(createddate) as quarter,
    YEAR(createddate) as year
FROM datalake_raw.salesforce_survey
