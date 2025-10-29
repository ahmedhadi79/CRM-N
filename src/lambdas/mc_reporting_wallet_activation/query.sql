SELECT
    *,
    case
        when rn_first = 1 then 'Yes'
        else 'NO'
    end as first_activation
FROM
    (
        SELECT
            *
        FROM
            (
                SELECT
                    distinct b.dynamodb_new_image_user_id_s,
                    a.dynamodb_new_image_token_n,
                    a.dynamodb_new_image_updated_at_n,
                    dynamodb_new_image_message_m_iso_msg_m_de124_m_1_s,
                    row_number() OVER (
                        partition by a.dynamodb_new_image_token_n
                        ORDER BY
                            a.dynamodb_new_image_updated_at_n desc
                    ) as rn,
                    row_number() OVER (
                        partition by a.dynamodb_new_image_token_n
                        ORDER BY
                            a.dynamodb_new_image_updated_at_n asc
                    ) as rn_first,
                    dynamodb_new_image_message_m_iso_msg_m_de124_m_3_s,
                    dynamodb_new_image_message_m_iso_msg_m_de124_m_7_s,
                    dynamodb_new_image_message_m_iso_msg_m_de48_m_26_s,
                    case
                        when dynamodb_new_image_message_m_iso_msg_m_de48_m_26_s = '103' then 'Apple Pay'
                        else 'Google Pay'
                    end as Wallet_Ind,
                    case
                        when dynamodb_new_image_message_m_iso_msg_m_de124_m_7_s in ('1', '2') then 'Approve'
                        else null
                    end as Approved_Ind,
                    email_notifications_m_marketing,
                    push_notifications_m_marketing,
                    sms_notifications_m_marketing
                FROM
                    datalake_curated.dynamo_card_fast_messages_default as a
                    INNER JOIN datalake_curated.dynamo_sls_cards as b on a.dynamodb_new_image_token_n = b.dynamodb_new_image_token_n
                    LEFT JOIN datalake_curated.customer_timeline_detail as c on b.dynamodb_new_image_user_id_s = c.dynamo_user_key
                WHERE
                    --dynamodb_new_image_message_m_iso_msg_m_de124_m_1_s = 'TC' ---authentication
                    --and 
                    dynamodb_new_image_message_m_iso_msg_m_de48_m_26_s is not null
                    and dynamodb_new_image_message_m_iso_msg_m_de48_m_26_s <> '' ---removing filter needed
                    and dynamodb_new_image_message_m_iso_msg_m_de48_m_26_s in ('103', '216')
                    and dynamodb_new_image_message_m_iso_msg_m_de124_m_3_s in ('01', '7')
                    and dynamodb_new_image_message_m_iso_msg_m_de124_m_7_s in ('1', '2') --google and apple pay
                    --and dynamodb_new_image_message_m_iso_msg_m_de124_m_7_s in ('1') -- approve
                    --and dynamodb_new_image_message_m_iso_msg_m_de124_m_3_s = '03'
            ) as a
        WHERE
            rn = 1
    ) as b
WHERE
    email_notifications_m_marketing = true
    or push_notifications_m_marketing = true
    or sms_notifications_m_marketing = true -- dynamodb_new_image_user_id_s = 'hJUrGrCUZ6XfrB6DFybvi2' and
    -- and 
    --rn = 1