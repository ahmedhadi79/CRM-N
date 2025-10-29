with cases as (
    select case_rt_dev_name__c,
        case_number,
        type,
        status,
        origin,
        owner_id,
        contact_id,
        subject,
        complaint_status__c as complaint_status,
        response_status__c as response_status,
        redress_paid__c as redress_paid,
        redress_amount__c as redress_amount,
        complaint_root_cause__c as complaint_root_cause,
        createddate
    from salesforce_cases
    where case_rt_dev_name__c = 'Complaint'
),
sfuser as (
    Select id,
        name
    from salesforce_user
),
sfcontact as (
    Select id,
        name
    from salesforce_contact
)
select cs.case_rt_dev_name__c as case_record_type,
    cs.case_number,
    cs.type,
    cs.status,
    cs.complaint_status,
    cs.response_status,
    cs.redress_paid,
    cs.redress_amount,
    cs.complaint_root_cause,
    cs.origin as case_origin,
    u.name as case_owner,
    ct.name as name,
    cs.subject,
    cs.createddate as date_time_opened
from cases cs
    inner join sfuser u on (cs.owner_id = u.id)
    inner join sfcontact ct on (cs.contact_id = ct.id)
