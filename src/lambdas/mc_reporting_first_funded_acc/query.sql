----- CTE all_cuts to get the Customer ID's, account status and date fo different status  -------
WITH all_cust AS (
    SELECT
        COALESCE(
            dynamodb_key_id_s,
            dynamodb_keys_id_s,
            dynamodb_new_image_id_s
        ) AS user_id,
        dynamodb_new_image_status_s,
        COALESCE(
            NULLIF(
                NULLIF(LTRIM(RTRIM(dynamodb_new_image_brand_id_s)), ''),
                '<NA>'
            ),
            'NOMO_BANK'
        ) AS brand_id,
        COALESCE(
            NULLIF(
                NULLIF(
                    LTRIM(
                        RTRIM(
                            dynamodb_new_image_individual_m_address_m_country_code_s
                        )
                    ),
                    ''
                ),
                '<NA>'
            ),
            ''
        ) AS Country,
        dynamodb_new_image_updated_at_n,
        ROW_NUMBER() OVER (
            PARTITION BY COALESCE(
                dynamodb_key_id_s,
                dynamodb_keys_id_s,
                dynamodb_new_image_id_s
            )
            ORDER BY
                dynamodb_new_image_updated_at_n DESC,
                dynamodb_approximate_creation_date_time DESC
        ) AS rn_last,
        ROW_NUMBER() OVER (
            PARTITION BY COALESCE(
                dynamodb_key_id_s,
                dynamodb_keys_id_s,
                dynamodb_new_image_id_s
            ),
            dynamodb_new_image_status_s
            ORDER BY
                dynamodb_new_image_updated_at_n ASC,
                dynamodb_approximate_creation_date_time ASC
        ) AS rn_first_status
    FROM
        datalake_curated.dynamo_scv_sls_customers
    WHERE
        DATE(dynamodb_new_image_updated_at_n) >= DATE('2021-07-01')
),
----- CTE apprv_cust -> filter by first row of status 'APPROVED' from CTE all_cust
apprv_cust AS (
    SELECT
        *
    FROM
        all_cust
    WHERE
        rn_last = 1
        AND dynamodb_new_image_status_s = 'APPROVED'
),
----- CTE Funded_Date_2_CTE -> extract the fields by joining tables 'datalake_raw.mambu_deposit_accounts', 'datalake_raw.mambu_deposit_transactions'
Funded_Date_2_CTE AS (
    SELECT
        a.*,
        t.type as Type_Funding,
        t.amount as Funded_Amount,
        t.GBP_Amount,
        t.transaction_details_transaction_channel_id,
        -- fields to use in table from table datalake_raw.mambu_deposit_transactions
        t.Type_Transaction,
        t.currency_code as Currency_Code,
        -- fields to use in table from table datalake_raw.mambu_deposit_transactions
        c.push_notifications_m_marketing as HasOptedOutOfPush,
        -- field to use in table and tell if customer receives notification or not (True/False)
        b.id AS Customer_ID,
        ROW_NUMBER() OVER (
            PARTITION BY account_holder_key
            ORDER BY
                value_date
        ) AS Funded_Date_2,
        -- get the first customer based on the value_date (datalake_raw.mambu_deposit_transactions)
        value_date as First_Date_Funded
    FROM
        (
            SELECT
                *
            FROM
                (
                    SELECT
                        DISTINCT encoded_key,
                        id,
                        name,
                        account_holder_key,
                        account_state,
                        account_type,
                        balances_total_balance,
                        currencycloud_deposit_accounts_cc_account_pool_da,
                        last_modified_date,
                        ROW_NUMBER() OVER (
                            PARTITION BY account_holder_key
                            ORDER BY
                                last_modified_date
                        ) AS Funded_Date
                    FROM
                        datalake_raw.mambu_deposit_accounts
                    WHERE
                        CAST(balances_total_balance AS DECIMAL(18, 3)) > 0
                )
            WHERE
                Funded_Date = 1
        ) AS a
        INNER JOIN (
            SELECT
                DISTINCT id,
                encoded_key
            FROM
                datalake_raw.mambu_clients
        ) b ON a.account_holder_key = b.encoded_key -- join to get Customer ID from table 'datalake_raw.mambu_clients'
        INNER JOIN (
            SELECT
                DISTINCT user_id,
                brand_id,
                Country
            FROM
                apprv_cust
        ) c ON b.id = c.user_id
        INNER JOIN (
            SELECT
                *
            FROM
                (
                    SELECT
                        DISTINCT parent_account_key,
                        ROW_NUMBER() OVER (
                            PARTITION BY id
                            ORDER BY
                                timestamp_extracted
                        ) AS rn,
                        value_date,
                        amount,
                        currency_code,
                        case
                            when currency_code = 'USD' then cast(amount as decimal (18, 3)) * 0.80
                            when currency_code = 'EUR' then cast(amount as decimal (18, 3)) * 0.86
                            when currency_code = 'AED' then cast(amount as decimal (18, 3)) * 0.22
                            when currency_code = 'KWD' then cast(amount as decimal (18, 3)) * 2.65
                            when currency_code = 'SAR' then cast(amount as decimal (18, 3)) * 0.22
                            else cast(amount as decimal (18, 3))
                        end as GBP_Amount,
                        type,
                        CASE
                            WHEN transaction_details_transaction_channel_id IS NULL THEN 'Unknown'
                            ELSE transaction_details_transaction_channel_id
                        End as Type_Transaction,
                        -- remove 'NULL' value by changing to 'Unknown'
                        transaction_details_transaction_channel_id
                    FROM
                        datalake_raw.mambu_deposit_transactions
                    WHERE
                        type = 'DEPOSIT'
                )
            WHERE
                rn = 1
        ) AS t ON a.encoded_key = t.parent_account_key
        LEFT JOIN (
            SELECT
                DISTINCT dynamo_user_key,
                push_notifications_m_marketing
            FROM
                datalake_curated.customer_timeline_detail
        ) AS c ON c.dynamo_user_key = b.id
)
SELECT
    *
FROM
    Funded_Date_2_CTE
WHERE
    Funded_Date_2 = 1
    AND -- filter to get the first funded date
    First_Date_Funded >= date_add('day', -3, CURRENT_DATE)
ORDER BY
    account_holder_key,
    last_modified_date
