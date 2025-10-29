with cards_fast_messages as (
    SELECT
        dynamodb_new_image_created_at_n AS card_message_datetime,
        cast(dynamodb_new_image_updated_at_n as date) AS card_message_date,
        dynamodb_new_image_message_m_message_type_m_message_desc_s AS card_message_description,
        dynamodb_new_image_message_m_summary_m_processor_decision_desc_s AS card_message_processor_decision,
        dynamodb_new_image_message_m_summary_m_billing_currency_s AS card_message_billing_currency,
        dynamodb_new_image_message_m_summary_m_billing_amount_s AS card_message_billing_amount,
        dynamodb_new_image_message_m_summary_m_transaction_currency_s AS card_message_transaction_currency,
        dynamodb_new_image_message_m_summary_m_transaction_amount_s AS card_message_transaction_amount,
        dynamodb_new_image_token_n AS card_message_token,
        dynamodb_new_image_message_m_summary_m_spend_type_s AS card_message_spend_type_category,
        dynamodb_new_image_message_m_summary_m_spend_location_s AS card_message_spend_location,
        dynamodb_new_image_message_m_iso_msg_m_de39_s,
        dynamodb_new_image_message_m_message_type_m_message_type_s,
        dynamodb_new_image_message_m_iso_msg_m_de124_m_1_s,
        CASE
            WHEN dynamodb_new_image_message_m_iso_msg_m_de48_m_26_s = '103' THEN 'Apple_Pay'
            WHEN dynamodb_new_image_message_m_iso_msg_m_de48_m_26_s = '216' THEN 'Google_Pay'
            WHEN dynamodb_new_image_message_m_iso_msg_m_de48_m_26_s = '217' THEN 'Samsung_Pay'
            WHEN dynamodb_new_image_message_m_iso_msg_m_de48_m_26_s = '327' THEN 'Merchant_tokenization_program'
            ELSE dynamodb_new_image_message_m_iso_msg_m_de48_m_26_s
        END as wallet_indicator
    FROM
        datalake_curated.dynamo_card_fast_messages_default
    where
        dynamodb_new_image_message_m_iso_msg_m_de48_m_26_s is not null
        and dynamodb_new_image_message_m_iso_msg_m_de48_m_26_s <> ''
        and dynamodb_new_image_message_m_iso_msg_m_de48_m_26_s in ('103', '216')
),
cards_rn as (
    SELECT
        *,
        cards_status AS Customer_Latest_Card_Status,
        case
            when cards_status = 'ACTIVE' THEN 1
            ELSE 0
        END AS Active_Card
    FROM
        (
            select
                dynamodb_keys_id_s as cards_id_dynamo,
                dynamodb_new_image_updated_at_n as cards_updated_datetime,
                timestamp_extracted as cards_timestamp_extracted,
                dynamodb_new_image_token_n as cards_token,
                dynamodb_new_image_user_id_s as cards_customer_id,
                dynamodb_new_image_state_s as cards_status,
                ROW_NUMBER () OVER (
                    PARTITION BY dynamodb_new_image_user_id_s
                    ORDER BY
                        dynamodb_new_image_updated_at_n DESC,
                        timestamp_extracted DESC
                ) as cards_row_number
            from
                datalake_raw.dynamo_sls_cards
        )
    where
        cards_row_number = 1
),
--- A) THIS IS FOR ALL APPROVED TRANSACTIONS MADE BY EACH CUSTOMERS SINCE BEING ONBOARDED (ALL TIME) -----
Transactions as (
    SELECT
        cards_customer_id as Customer_ID,
        Wallet_Indicator,
        MAX(card_message_date) AS Latest_Transaction_Wallet_Date
    from
        cards_fast_messages
        left outer join cards_rn on cards_rn.cards_token = cards_fast_messages.card_message_token
    where
        COALESCE(
            dynamodb_new_image_message_m_iso_msg_m_de124_m_1_s,
            'XX'
        ) not in ('AC', 'TC', 'TV', 'TA', 'TE')
        and cast(
            dynamodb_new_image_message_m_iso_msg_m_de39_s as decimal(18, 3)
        ) = 0
        and cast(
            dynamodb_new_image_message_m_message_type_m_message_type_s as decimal(18, 3)
        ) in (100, 120) -- ALL 3 FILTERS ARE TO FILTER FOR APPROVED TRANSACTIONS
        and wallet_indicator in ('Apple_Pay', 'Google_Pay')
    group by
        1,
        2
    order by
        cards_customer_id
)
SELECT
    *
FROM
    Transactions