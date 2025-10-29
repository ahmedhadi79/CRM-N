with accounthistory as (
    SELECT field,
        old_value,
        new_value,
        account_id,
        createddate
    FROM salesforce_accounthistory
    WHERE field in (
            'Vulnerability_Comments__pc',
            'Vulnerable_Customer__pc',
            'Vulnerable_Customer_Subcategory__pc'
        )
    ORDER BY createddate DESC
),
account as (
    Select id,
        name
    from salesforce_account
)
select ah.field as field,
    ah.old_value as old_value,
    ah.new_value as new_value,
    ah.account_id as account_id,
    a.name as account_name,
    ah.createddate as created_date
from accounthistory ah
    inner join account a on (ah.account_id = a.id)
