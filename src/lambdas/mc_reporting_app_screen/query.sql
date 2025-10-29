SELECT
    *
FROM
    (
        SELECT
            amplitude_id,
            event_id,
            event_time,
            event_type,
            server_received_time,
            server_upload_time,
            session_id,
            user_id,
            platform,
            region,
            device_type,
            country,
            language,
            device_family,
            brandid,
            email_notifications_m_marketing,
            push_notifications_m_marketing,
            sms_notifications_m_marketing,
            row_number() OVER (
                partition by user_id,
                amplitude_id,
                event_time,
                event_type
                order by
                    event_time desc
            ) as rn
        FROM
            datalake_curated.mir_amplitude as a
            LEFT JOIN datalake_curated.customer_timeline_detail as b on a.user_id = b.dynamo_user_key
        WHERE
            lower(event_type) IN (
                'screen_prop_fin_type',
                'screen_get_more_with_nomo',
                'screen_fixed_term_deposit_calculator',
                'screen_rent_or_residential',
                'screen_btldashboard',
                'screen_legal_agreement',
                'screen_add_new_savings_account_choices',
                'screen_instant_access_saver_benefits',
                'screen_instant_access_saver_calculator',
                'screen_pay_and_transfer_overlay',
                'screen_select_currency_account_variant_b'
            )
            and date(event_time) >= date_add('day', -3, CURRENT_DATE)
            and (
                email_notifications_m_marketing = true
                or push_notifications_m_marketing = true
                or sms_notifications_m_marketing = true
            )
    ) as a
WHERE
    rn = 1
ORDER BY
    event_time DESC