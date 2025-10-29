WITH ranked_mortgage_apps AS (
     SELECT
          salesforce_case_id,
          id,
          loan_account_mambu_id,
          ROW_NUMBER() OVER (
               PARTITION BY salesforce_case_id
               ORDER BY
                    updated_at DESC
          ) AS rn
     FROM
          datalake_raw.rds_mortgage_applications
     WHERE
          loan_account_mambu_id IS NOT NULL
          AND salesforce_case_id IS NOT NULL
),
latest_mortgage_apps AS (
     SELECT
          salesforce_case_id,
          id,
          loan_account_mambu_id
     FROM
          ranked_mortgage_apps
     WHERE
          rn = 1
),
loan_accounts AS (
     SELECT
          encoded_key,
          mortgage_app_mortgage_application_id,
          id,
          balances_principal_balance,
          ROW_NUMBER() OVER (
               PARTITION BY encoded_key
               ORDER BY
                    last_modified_date DESC
          ) AS rn
     FROM
          datalake_raw.mambu_loan_accounts
),
latest_loan_accounts AS (
     SELECT
          encoded_key,
          mortgage_app_mortgage_application_id,
          id,
          balances_principal_balance
     FROM
          loan_accounts
     WHERE
          rn = 1
),
latest_installments AS (
     SELECT
          parent_account_key,
          due_date,
          state,
          principal_amount_due,
          interest_amount_due
     FROM
          datalake_raw.mambu_loan_accounts_installments
     WHERE
          timestamp_extracted =(
               SELECT
                    MAX(timestamp_extracted)
               FROM
                    datalake_raw.mambu_loan_accounts_installments
          )
),
latest_installments_with_id AS (
     SELECT
          la.id as loan_account_id,
          li.due_date,
          li.state,
          li.principal_amount_due,
          li.interest_amount_due
     FROM
          latest_installments AS li
          LEFT JOIN latest_loan_accounts AS la ON li.parent_account_key = la.encoded_key
),
installment_sums AS (
     SELECT
          loan_account_id,
          SUM(
               CASE
                    WHEN state = 'PENDING'
                    AND due_date < CURRENT_DATE THEN COALESCE(principal_amount_due, 0) + COALESCE(interest_amount_due, 0)
                    ELSE 0
               END
          ) AS current_payment_shortfall
     FROM
          latest_installments_with_id
     GROUP BY
          loan_account_id
),
ranked_latest_installments_with_id AS (
     SELECT
          *,
          ROW_NUMBER() OVER (
               PARTITION BY loan_account_id
               ORDER BY
                    due_date DESC
          ) AS rn
     FROM
          latest_installments_with_id
),
installment_summary AS (
     SELECT
          li.loan_account_id,
          li.due_date AS max_due_date,
          li.state AS max_state,
          li.principal_amount_due,
          li.interest_amount_due,
          (li.principal_amount_due + li.interest_amount_due) AS expected_monthly_payment,
          isum.current_payment_shortfall
     FROM
          ranked_latest_installments_with_id li
          LEFT JOIN installment_sums isum ON li.loan_account_id = isum.loan_account_id
     WHERE
          li.rn = 1
)
SELECT
     isum.expected_monthly_payment AS current_expected_monthly_payment__c,
     isum.current_payment_shortfall AS current_payment_shortfall__c,
     DATE_DIFF('month', CURRENT_DATE, isum.max_due_date) AS remaining_term_months__c,
     CASE
          WHEN isum.max_state = 'PENDING'
          AND isum.max_due_date < CURRENT_DATE THEN DATE_DIFF('month', isum.max_due_date, CURRENT_DATE)
          ELSE 0
     END AS months_past_maturity__c,
     lla.balances_principal_balance AS current_balance_outstanding__c,
     s.*
FROM
     TABLE(
          exclude_columns(
               input => TABLE(datalake_raw.salesforce_psd_form_c),
               columns => DESCRIPTOR(
                    current_expected_monthly_payment__c,
                    current_payment_shortfall__c,
                    remaining_term_months__c,
                    months_past_maturity__c,
                    current_balance_outstanding__c
               )
          )
     ) s
     LEFT JOIN latest_mortgage_apps m ON s.case__c = m.salesforce_case_id
     LEFT JOIN latest_loan_accounts lla ON lla.mortgage_app_mortgage_application_id = m.id
     and lla.id = m.loan_account_mambu_id
     LEFT JOIN installment_summary isum ON lla.id = isum.loan_account_id
