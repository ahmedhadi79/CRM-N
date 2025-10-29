select
id,
customer_reference_id__c,
createddate,
lastmodifieddate,
closeddate,
coalesce(NULLIF(type, ''), NULLIF(case_record_type_name__c, '') ,'Unknown') as type,
CASE
    WHEN closeddate is null THEN 'OPEN'
    WHEN closeddate is not null THEN 'CLOSED'
END as open_closed,
DAY(createddate) as day,
MONTH(createddate) as month,
QUARTER(createddate) as quarter,
YEAR(createddate) as year
FROM datalake_raw.salesforce_cases
