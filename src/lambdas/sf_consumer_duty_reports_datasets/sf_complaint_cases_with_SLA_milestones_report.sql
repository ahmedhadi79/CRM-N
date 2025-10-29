with cases as (
    select id,case_rt_dev_name__c,
        case_number,
        type,
        status,
        origin,
        priority,
        createddate
    from salesforce_cases
    where case_rt_dev_name__c = 'Complaint'
),
casemilestone as (
    Select id,
        case_id,
        milestone_type_id,
        target_date,
        completion_date
    from salesforce_casemilestone
),
milestonetype as (
    Select id,
        name
    from salesforce_milestonetype
)
select c.case_number,
    c.status,
    c.priority,
    c.origin as case_origin,
    c.createddate as date_time_opened,
    mt.name as milestone,
    cm.target_date,
    cm.completion_date,
    c.case_rt_dev_name__c as case_record_type
from casemilestone cm
    inner join cases c on (cm.case_id = c.id)
    inner join milestonetype mt on (mt.id = cm.milestone_type_id)
