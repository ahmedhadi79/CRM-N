with -- get all ftd products and their related details such as rates etc.
default_ftd as (
    select
        regexp_replace(dynamodb_new_image_sk_s, ':default', '') as default_account_type,
        regexp_replace(
            dynamodb_new_image_sk_s,
            ':post_maturity_reinvestment',
            ''
        ) as reinvest_account_type,
        dynamodb_new_image_sk_s,
        dynamodb_new_image_product_encoded_key_s as product_key,
        dynamodb_new_image_min_amount_n as tier_0_min_amount -- , cast(dynamodb_new_image_rates_l_0_m_rate_n as decimal(18, 2)) as tier_0_rate
,
        coalesce(
            (
                cast(
                    dynamodb_new_image_interest_rate_n as decimal(18, 2)
                ) - cast(
                    dynamodb_new_image_rates_l_0_m_profit_margin_n as decimal(18, 2)
                )
            ),
            cast(
                dynamodb_new_image_rates_l_0_m_rate_n as decimal(18, 2)
            )
        ) as tier_0_rate,
        cast(
            dynamodb_new_image_rates_l_0_m_bonus_rate_n as decimal(18, 2)
        ) as tier_0_bonus_rate,
        dynamodb_new_image_rates_l_1_m_min_amount_n as tier_1_min_amount -- , cast(dynamodb_new_image_rates_l_1_m_rate_n as decimal(18, 2)) as tier_1_rate
,
        coalesce(
            (
                cast(
                    dynamodb_new_image_interest_rate_n as decimal(18, 2)
                ) - cast(
                    dynamodb_new_image_rates_l_1_m_profit_margin_n as decimal(18, 2)
                )
            ),
            cast(
                dynamodb_new_image_rates_l_1_m_rate_n as decimal(18, 2)
            )
        ) as tier_1_rate,
        cast(
            dynamodb_new_image_rates_l_1_m_bonus_rate_n as decimal(18, 2)
        ) as tier_1_bonus_rate,
        ROW_NUMBER() over(
            PARTITION by dynamodb_new_image_product_encoded_key_s,
            dynamodb_new_image_sk_s
            order by
                from_unixtime(cast(dynamodb_new_image_updated_at_n as BIGINT)) desc
        ) as rn_last
    from
        datalake_raw.dynamo_default_payment_products a
    where
        1 = 1
        and dynamodb_new_image_sk_s like '%:ftd_%'
    order by
        dynamodb_new_image_sk_s
),
-- get the latest instance of each ftd product
latest_ftd as (
    select
        *
    from
        default_ftd
    where
        rn_last = 1
),
-- get the default product
standard_ftd as (
    select
        default_account_type,
        dynamodb_new_image_sk_s,
        product_key,
        tier_0_min_amount,
        tier_0_rate,
        tier_1_min_amount,
        tier_1_rate
    from
        latest_ftd a
    where
        1 = 1
        and dynamodb_new_image_sk_s like '%default'
),
-- select * from standard_ftd
-- get the reinvest product
reinvest_ftd as (
    select
        reinvest_account_type,
        dynamodb_new_image_sk_s,
        product_key,
        tier_0_bonus_rate,
        tier_1_bonus_rate -- bonus rate will need to be changed once available
,
        tier_0_min_amount,
        tier_1_min_amount
    from
        latest_ftd a
    where
        1 = 1
        and dynamodb_new_image_sk_s like '%reinvestment'
),
-- select * from reinvest_ftd
-- join default product with reinvest product to the preferential interest rate per deposit amount and term.
reinvest_ftd_get_pref_rate as (
    select
        reinvest_account_type as account_type,
        a.dynamodb_new_image_sk_s,
        a.product_key,
        SUBSTRING(reinvest_account_type, 1, 3) as currency,
        a.tier_0_min_amount,
        a.tier_1_min_amount,
        tier_0_rate as tier_0_default_rate,
        tier_1_rate as tier_1_default_rate,
        a.tier_0_bonus_rate + tier_0_rate as tier_0_pref_rate -- pref_rate
,
        a.tier_1_bonus_rate + tier_1_rate as tier_1_pref_rate -- pref_rate
    from
        reinvest_ftd as a
        inner join standard_ftd as b on a.reinvest_account_type = b.default_account_type
        and a.tier_1_min_amount = b.tier_1_min_amount
),
tier_0 as (
    select
        '0' as tier,
        account_type,
        currency,
        tier_0_min_amount as min_amount,
        tier_0_default_rate as default_rate,
        tier_0_pref_rate as pref_rate -- pref_rate
    from
        reinvest_ftd_get_pref_rate as a
),
tier_1 as (
    select
        '1' as tier,
        account_type,
        currency,
        tier_1_min_amount as min_amount,
        tier_1_default_rate as default_rate,
        tier_1_pref_rate as pref_rate -- pref_rate
    from
        reinvest_ftd_get_pref_rate as a
),
unioned as (
    select
        *
    from
        tier_0
    UNION
    ALL
    select
        *
    from
        tier_1
),
unioned_agg as (
    select
        distinct currency,
        max(pref_rate) OVER (PARTITION BY currency) as top_pref_rate_per_currency,
        max(pref_rate) OVER () as top_pref_rate_overall
    from
        unioned
),
unioned_agg_pivot as (
    select
        concat(
            cast(
                max(
                    case
                        when currency = 'eur' then top_pref_rate_per_currency
                    end
                ) as varchar
            ),
            '%'
        ) as FTD_Top_Pref_Rate_Euro,
        concat(
            cast(
                max(
                    case
                        when currency = 'gbp' then top_pref_rate_per_currency
                    end
                ) as varchar
            ),
            '%'
        ) as FTD_Top_Pref_Rate_GBP,
        concat(
            cast(
                max(
                    case
                        when currency = 'usd' then top_pref_rate_per_currency
                    end
                ) as varchar
            ),
            '%'
        ) as FTD_Top_Pref_Rate_USD,
        concat(cast(max(top_pref_rate_overall) as varchar), '%') as FTD_Top_Pref_Rate_Overall
    from
        unioned_agg
),
combined_standard_reinvest_ftd as (
    select
        'a' as gb_col,
        account_type,
        dynamodb_new_image_sk_s,
        product_key,
        tier_0_min_amount,
        tier_1_min_amount,
        tier_0_pref_rate as rate_0,
        tier_1_pref_rate as rate_1
    from
        reinvest_ftd_get_pref_rate
    union
    all
    select
        'a' as gb_col,
        default_account_type as account_type,
        dynamodb_new_image_sk_s,
        product_key,
        tier_0_min_amount,
        tier_1_min_amount,
        tier_0_rate as rate_0,
        tier_1_rate as rate_1
    from
        standard_ftd
),
-- select * from combined_standard_reinvest_ftd
ftd_rate_pivot_rows_to_columns as(
    select
        -- GBP
        -- min amounts for rate 0
        max(
            case
                when dynamodb_new_image_sk_s = 'gbp:ftd_3m:default' then tier_0_min_amount
            end
        ) as FTD_GBP_Rate_0_Min_Amount -- min amounts for rate 1
,
        max(
            case
                when dynamodb_new_image_sk_s = 'gbp:ftd_3m:default' then tier_1_min_amount
            end
        ) as FTD_GBP_Rate_1_Min_Amount -- default rate for rate 0
,
        max(
            case
                when dynamodb_new_image_sk_s = 'gbp:ftd_3m:default' then concat(cast(rate_0 as varchar), '%')
            end
        ) as FTD_GBP_3m_Rate_0_Default_Rate,
        max(
            case
                when dynamodb_new_image_sk_s = 'gbp:ftd_6m:default' then concat(cast(rate_0 as varchar), '%')
            end
        ) as FTD_GBP_6m_Rate_0_Default_Rate,
        max(
            case
                when dynamodb_new_image_sk_s = 'gbp:ftd_12m:default' then concat(cast(rate_0 as varchar), '%')
            end
        ) as FTD_GBP_12m_Rate_0_Default_Rate -- default rate for rate 1
,
        max(
            case
                when dynamodb_new_image_sk_s = 'gbp:ftd_3m:default' then concat(cast(rate_1 as varchar), '%')
            end
        ) as FTD_GBP_3m_Rate_1_Default_Rate,
        max(
            case
                when dynamodb_new_image_sk_s = 'gbp:ftd_6m:default' then concat(cast(rate_1 as varchar), '%')
            end
        ) as FTD_GBP_6m_Rate_1_Default_Rate,
        max(
            case
                when dynamodb_new_image_sk_s = 'gbp:ftd_12m:default' then concat(cast(rate_1 as varchar), '%')
            end
        ) as FTD_GBP_12m_Rate_1_Default_Rate -- pref rate for rate 0
,
        max(
            case
                when dynamodb_new_image_sk_s = 'gbp:ftd_3m:post_maturity_reinvestment' then concat(cast(rate_0 as varchar), '%')
            end
        ) as FTD_GBP_3m_Rate_0_Preferential_Rate,
        max(
            case
                when dynamodb_new_image_sk_s = 'gbp:ftd_6m:post_maturity_reinvestment' then concat(cast(rate_0 as varchar), '%')
            end
        ) as FTD_GBP_6m_Rate_0_Preferential_Rate,
        max(
            case
                when dynamodb_new_image_sk_s = 'gbp:ftd_12m:post_maturity_reinvestment' then concat(cast(rate_0 as varchar), '%')
            end
        ) as FTD_GBP_12m_Rate_0_Preferential_Rate -- pref rate for rate 0
,
        max(
            case
                when dynamodb_new_image_sk_s = 'gbp:ftd_3m:post_maturity_reinvestment' then concat(cast(rate_1 as varchar), '%')
            end
        ) as FTD_GBP_3m_Rate_1_Preferential_Rate,
        max(
            case
                when dynamodb_new_image_sk_s = 'gbp:ftd_6m:post_maturity_reinvestment' then concat(cast(rate_1 as varchar), '%')
            end
        ) as FTD_GBP_6m_Rate_1_Preferential_Rate,
        max(
            case
                when dynamodb_new_image_sk_s = 'gbp:ftd_12m:post_maturity_reinvestment' then concat(cast(rate_1 as varchar), '%')
            end
        ) as FTD_GBP_12m_Rate_1_Preferential_Rate -- --------------------------------------------------------------------------------------------------------------
        -- usd
        -- min amounts for rate 0
,
        max(
            case
                when dynamodb_new_image_sk_s = 'usd:ftd_3m:default' then tier_0_min_amount
            end
        ) as FTD_USD_Rate_0_Min_Amount -- min amounts for rate 1
,
        max(
            case
                when dynamodb_new_image_sk_s = 'usd:ftd_3m:default' then tier_1_min_amount
            end
        ) as FTD_USD_Rate_1_Min_Amount -- default rate for rate 0
,
        max(
            case
                when dynamodb_new_image_sk_s = 'usd:ftd_3m:default' then concat(cast(rate_0 as varchar), '%')
            end
        ) as FTD_USD_3m_Rate_0_Default_Rate,
        max(
            case
                when dynamodb_new_image_sk_s = 'usd:ftd_6m:default' then concat(cast(rate_0 as varchar), '%')
            end
        ) as FTD_USD_6m_Rate_0_Default_Rate,
        max(
            case
                when dynamodb_new_image_sk_s = 'usd:ftd_12m:default' then concat(cast(rate_0 as varchar), '%')
            end
        ) as FTD_USD_12m_Rate_0_Default_Rate -- default rate for rate 1
,
        max(
            case
                when dynamodb_new_image_sk_s = 'usd:ftd_3m:default' then concat(cast(rate_1 as varchar), '%')
            end
        ) as FTD_USD_3m_Rate_1_Default_Rate,
        max(
            case
                when dynamodb_new_image_sk_s = 'usd:ftd_6m:default' then concat(cast(rate_1 as varchar), '%')
            end
        ) as FTD_USD_6m_Rate_1_Default_Rate,
        max(
            case
                when dynamodb_new_image_sk_s = 'usd:ftd_12m:default' then concat(cast(rate_1 as varchar), '%')
            end
        ) as FTD_USD_12m_Rate_1_Default_Rate -- preferential rate for rate 0
,
        max(
            case
                when dynamodb_new_image_sk_s = 'usd:ftd_3m:post_maturity_reinvestment' then concat(cast(rate_0 as varchar), '%')
            end
        ) as FTD_USD_3m_Rate_0_Preferential_Rate,
        max(
            case
                when dynamodb_new_image_sk_s = 'usd:ftd_6m:post_maturity_reinvestment' then concat(cast(rate_0 as varchar), '%')
            end
        ) as FTD_USD_6m_Rate_0_Preferential_Rate,
        max(
            case
                when dynamodb_new_image_sk_s = 'usd:ftd_12m:post_maturity_reinvestment' then concat(cast(rate_0 as varchar), '%')
            end
        ) as FTD_USD_12m_Rate_0_Preferential_Rate -- preferential rate for rate 1
,
        max(
            case
                when dynamodb_new_image_sk_s = 'usd:ftd_3m:post_maturity_reinvestment' then concat(cast(rate_1 as varchar), '%')
            end
        ) as FTD_USD_3m_Rate_1_Preferential_Rate,
        max(
            case
                when dynamodb_new_image_sk_s = 'usd:ftd_6m:post_maturity_reinvestment' then concat(cast(rate_1 as varchar), '%')
            end
        ) as FTD_USD_6m_Rate_1_Preferential_Rate,
        max(
            case
                when dynamodb_new_image_sk_s = 'usd:ftd_12m:post_maturity_reinvestment' then concat(cast(rate_1 as varchar), '%')
            end
        ) as FTD_USD_12m_Rate_1_Preferential_Rate --------------------------------------------------------------------------------------------------------------
        -- eur
        -- min amounts for rate 0
,
        max(
            case
                when dynamodb_new_image_sk_s = 'eur:ftd_3m:default' then tier_0_min_amount
            end
        ) as FTD_EUR_Rate_0_Min_Amount -- min amounts for rate 1
,
        max(
            case
                when dynamodb_new_image_sk_s = 'eur:ftd_3m:default' then tier_1_min_amount
            end
        ) as FTD_EUR_Rate_1_Min_Amount -- default rate for rate 0
,
        max(
            case
                when dynamodb_new_image_sk_s = 'eur:ftd_3m:default' then concat(cast(rate_0 as varchar), '%')
            end
        ) as FTD_EUR_3m_Rate_0_Default_Rate,
        max(
            case
                when dynamodb_new_image_sk_s = 'eur:ftd_6m:default' then concat(cast(rate_0 as varchar), '%')
            end
        ) as FTD_EUR_6m_Rate_0_Default_Rate,
        max(
            case
                when dynamodb_new_image_sk_s = 'eur:ftd_12m:default' then concat(cast(rate_0 as varchar), '%')
            end
        ) as FTD_EUR_12m_Rate_0_Default_Rate -- default rate for rate 1
,
        max(
            case
                when dynamodb_new_image_sk_s = 'eur:ftd_3m:default' then concat(cast(rate_1 as varchar), '%')
            end
        ) as FTD_EUR_3m_Rate_1_Default_Rate,
        max(
            case
                when dynamodb_new_image_sk_s = 'eur:ftd_6m:default' then concat(cast(rate_1 as varchar), '%')
            end
        ) as FTD_EUR_6m_Rate_1_Default_Rate,
        max(
            case
                when dynamodb_new_image_sk_s = 'eur:ftd_12m:default' then concat(cast(rate_1 as varchar), '%')
            end
        ) as FTD_EUR_12m_Rate_1_Default_Rate -- pref rate for rate 0
,
        max(
            case
                when dynamodb_new_image_sk_s = 'eur:ftd_3m:post_maturity_reinvestment' then concat(cast(rate_0 as varchar), '%')
            end
        ) as FTD_EUR_3m_Rate_0_Preferential_Rate,
        max(
            case
                when dynamodb_new_image_sk_s = 'eur:ftd_6m:post_maturity_reinvestment' then concat(cast(rate_0 as varchar), '%')
            end
        ) as FTD_EUR_6m_Rate_0_Preferential_Rate,
        max(
            case
                when dynamodb_new_image_sk_s = 'eur:ftd_12m:post_maturity_reinvestment' then concat(cast(rate_0 as varchar), '%')
            end
        ) as FTD_EUR_12m_Rate_0_Preferential_Rate -- pref rate for rate 1
,
        max(
            case
                when dynamodb_new_image_sk_s = 'eur:ftd_3m:post_maturity_reinvestment' then concat(cast(rate_1 as varchar), '%')
            end
        ) as FTD_EUR_3m_Rate_1_Preferential_Rate,
        max(
            case
                when dynamodb_new_image_sk_s = 'eur:ftd_6m:post_maturity_reinvestment' then concat(cast(rate_1 as varchar), '%')
            end
        ) as FTD_EUR_6m_Rate_1_Preferential_Rate,
        max(
            case
                when dynamodb_new_image_sk_s = 'eur:ftd_12m:post_maturity_reinvestment' then concat(cast(rate_1 as varchar), '%')
            end
        ) as FTD_EUR_12m_Rate_1_Preferential_Rate
    from
        combined_standard_reinvest_ftd
    group by
        gb_col
),
-- select * from ftd_rate_pivot_rows_to_columns
------------------------------------------------------------------------------------------------------------------------------------
all_cust as (
    select
        *
    from
        (
            SELECT
                COALESCE(
                    dynamodb_key_id_s,
                    dynamodb_keys_id_s,
                    dynamodb_new_image_id_s
                ) as user_id,
                dynamodb_new_image_status_s as customer_status,
                dynamodb_new_image_updated_at_n,
                COALESCE(
                    nullif(
                        nullif(ltrim(rtrim(dynamodb_new_image_brand_id_s)), ''),
                        '<NA>'
                    ),
                    'NOMO_BANK'
                ) as brand_id,
                COALESCE(
                    nullif(
                        ltrim(
                            rtrim(
                                dynamodb_new_image_individual_m_address_m_country_code_s
                            )
                        ),
                        ''
                    ),
                    ''
                ) as residence_country_code,
                row_number() over(
                    PARTITION by COALESCE(
                        dynamodb_key_id_s,
                        dynamodb_keys_id_s,
                        dynamodb_new_image_id_s
                    )
                    ORDER BY
                        dynamodb_new_image_updated_at_n desc,
                        dynamodb_approximate_creation_date_time desc
                ) AS rn_last
            FROM
                datalake_curated.dynamo_scv_sls_customers
        ) a
    where
        1 = 1
        and rn_last = 1
),
deposit_account as (
    select
        b.id as user_id,
        a.encoded_key,
        name,
        account_holder_key,
        a.id as account_id,
        account_type as Account_Type,
        account_state,
        a.currency_code,
        cast(
            from_iso8601_timestamp(a.last_modified_date) as timestamp
        ) as last_modified_date,
        cast(
            from_iso8601_timestamp(a.approved_date) as timestamp
        ) as approved_date,
        cast(
            from_iso8601_timestamp(a.maturity_date) as timestamp
        ) as maturity_date,
        cast(
            from_iso8601_timestamp(a.activation_date) as timestamp
        ) as activation_date,
        cast(
            from_iso8601_timestamp(a.closed_date) as timestamp
        ) as closed_date,
        cast(balances_total_balance as decimal (18, 3)) as balance,
        interest_settings_interest_rate_settings_interest_rate,
        row_number() over(
            partition by account_holder_key,
            a.id
            order by
                from_iso8601_timestamp(a.last_modified_date) desc
        ) as rn_last,
        cast(
            accrued_amounts_interest_accrued as decimal(18, 3)
        ) as Interest_Accrued_Amount,
        case
            when currency_code = 'USD' then cast(balances_total_balance as decimal (18, 3)) * 0.80
            when currency_code = 'EUR' then cast(balances_total_balance as decimal (18, 3)) * 0.86
            when currency_code = 'AED' then cast(balances_total_balance as decimal (18, 3)) * 0.22
            when currency_code = 'KWD' then cast(balances_total_balance as decimal (18, 3)) * 2.65
            when currency_code = 'SAR' then cast(balances_total_balance as decimal (18, 3)) * 0.22
            else cast(balances_total_balance as decimal (18, 3))
        end as gbp_balance
    from
        datalake_raw.deposit_accounts a
        left join (
            select
                distinct id,
                encoded_key
            from
                datalake_raw.clients
        ) b on a.account_holder_key = b.encoded_key
),
latest_account_state as(
    select
        user_id,
        account_id,
        account_state
    from
        deposit_account
    where
        1 = 1
        and name like '%Fixed%'
        and rn_last = 1
),
get_ftd_terms as (
    select
        user_id,
        encoded_key,
        account_state,
        account_id,
        name,
        account_holder_key,
        maturity_date,
        approved_date,
        balance,
        gbp_balance,
        Account_Type,
        activation_date,
        interest_settings_interest_rate_settings_interest_rate as FTD_Interest_Rate,
        Interest_Accrued_Amount,
        currency_code,
        maturity_date + interval '1' hour as New_Maturity_Date,
        LEAD(approved_date) OVER(
            PARTITION BY account_holder_key
            ORDER BY
                approved_date
        ) AS Lead_Approved_Date,
        LEAD(account_id) OVER(
            PARTITION BY account_holder_key
            ORDER BY
                approved_date
        ) AS FTD_Next_Account_ID,
        case
            when (
                date_diff('month', activation_date, maturity_date) + 1
            ) = 4 then '3-months'
            else cast(
                (
                    date_diff('month', activation_date, maturity_date) + 1
                ) as varchar
            ) || '-months'
        end as ftd_term
    from
        (
            select
                *,
                row_number() over(
                    partition by account_holder_key,
                    account_id
                    order by
                        last_modified_date desc
                ) as rn_
            from
                deposit_account
            where
                1 = 1
                and name like '%Fixed%'
                and account_state = 'ACTIVE'
                and account_id not in (
                    select
                        distinct account_id
                    from
                        deposit_account
                    where
                        closed_date < maturity_date
                )
        ) a
    where
        1 = 1
        and rn_ = 1
        and maturity_date is not null -- when the maturity is null, the ftd is closed before the maturity date (2 accounts)
),
deposit_transaction as(
    select
        user_id,
        customer_status,
        brand_id,
        residence_country_code,
        name as account_name,
        e.ftd_term,
        account_id,
        maturity_date,
        New_Maturity_Date,
        FTD_Interest_Rate,
        Interest_Accrued_Amount,
        cast(from_iso8601_timestamp(value_date) as timestamp) as transaction_date,
        a.id as transaction_id,
        currency_code,
        CAST(amount as decimal(18, 3)) as amount,
        case
            when currency_code = 'USD' then cast(amount as decimal (18, 3)) * 0.80
            when currency_code = 'EUR' then cast(amount as decimal (18, 3)) * 0.86
            when currency_code = 'AED' then cast(amount as decimal (18, 3)) * 0.22
            when currency_code = 'KWD' then cast(amount as decimal (18, 3)) * 2.65
            when currency_code = 'SAR' then cast(amount as decimal (18, 3)) * 0.22
            else cast(amount as decimal (18, 3))
        end as gbp_amount,
        type,
        transaction_details_transaction_channel_id
    from
        (
            select
                *
            from
                (
                    select
                        *,
                        row_number() over(
                            partition by id
                            order by
                                timestamp_extracted desc
                        ) as rn
                    from
                        datalake_raw.deposit_transactions
                ) tb
            where
                rn = 1
        ) a
        inner join (
            select
                distinct account_holder_key,
                encoded_key,
                name
            from
                datalake_raw.deposit_accounts
        ) b on a.parent_account_key = b.encoded_key
        inner join (
            select
                distinct id as client_id,
                encoded_key
            from
                datalake_raw.clients
        ) c on b.account_holder_key = c.encoded_key
        inner join (
            select
                distinct user_id,
                brand_id,
                residence_country_code,
                customer_status
            from
                all_cust
        ) d on c.client_id = d.user_id
        inner join (
            select
                distinct encoded_key,
                account_id,
                ftd_term,
                maturity_date,
                New_Maturity_Date,
                FTD_Interest_Rate,
                Interest_Accrued_Amount
            from
                get_ftd_terms
        ) e on a.parent_account_key = e.encoded_key
),
matured_ftd_interest as(
    select
        user_id,
        account_id,
        amount as Interest_Accrued_Amount
    from
        deposit_transaction
    where
        1 = 1
        and type = 'INTEREST_APPLIED'
),
matured_ftd_amount as(
    select
        user_id,
        account_id,
        abs(amount) as FTD_Balance_at_Maturity,
        abs(gbp_amount) as FTD_Balance_at_Maturity_in_GBP
    from
        deposit_transaction
    where
        1 = 1
        and type = 'WITHDRAWAL'
),
maturity_date_manipulation as(
    select
        *,
        DATE_ADD('day', 1, maturity_date) as Maturity_date_plus_1days,
        DATE_ADD('day', 3, maturity_date) as Maturity_date_plus_3days,
        DATE_ADD('day', 7, maturity_date) as Maturity_date_plus_7days,
        DATE_ADD('day', 14, maturity_date) as Maturity_date_plus_14days,
        date_diff(
            'day',
            date(approved_date),
            date(Lead_Approved_Date)
        ) as Number_Days_Between_Approved,
        date_diff(
            'day',
            date(Lead_Approved_Date),
            date(maturity_date)
        ) as Number_Days_Between_Mat_Approved
    from
        get_ftd_terms
    order by
        account_holder_key,
        approved_date -- 'TRANSFER'
),
label_maturity_date as(
    select
        *,
        CASE
            WHEN date(Lead_Approved_Date) BETWEEN date(Maturity_Date)
            AND date(Maturity_date_plus_1days) THEN 1
            ELSE 0
        END AS FTD_Opened_Within_1_Days_Maturity,
        CASE
            WHEN date(Lead_Approved_Date) BETWEEN date(Maturity_Date)
            AND date(Maturity_date_plus_3days) THEN 1
            ELSE 0
        END AS FTD_Opened_Within_3_Days_Maturity,
        CASE
            WHEN date(Lead_Approved_Date) BETWEEN date(Maturity_Date)
            AND date(Maturity_date_plus_7days) THEN 1
            ELSE 0
        END AS FTD_Opened_Within_7_Days_Maturity,
        CASE
            WHEN date(Lead_Approved_Date) BETWEEN date(Maturity_Date)
            AND date(Maturity_date_plus_14days) THEN 1
            ELSE 0
        END AS FTD_Opened_Within_14_Days_Maturity
    from
        maturity_date_manipulation
),
per_account_level as(
    select
        a.user_id as Customer_ID,
        a.account_id as FTD_Account_ID,
        Account_Type,
        a.FTD_Interest_Rate,
        ftd_term as FTD_Term,
        maturity_date as FTD_Maturity_Date,
        New_Maturity_Date as FTD_Maturity_Date_plus_1hr,
        name as Name,
        currency_code as Currency_Code,
        account_holder_key as Account_Holder_Key,
        a.approved_date as FTD_Open_Date,
        balance as FTD_Balance,
        gbp_balance as FTD_Balance_in_GBP,
        Lead_Approved_Date as FTD_Next_Approved_Date,
        FTD_Next_Account_ID,
        FTD_Opened_Within_1_Days_Maturity,
        FTD_Opened_Within_3_Days_Maturity,
        FTD_Opened_Within_7_Days_Maturity,
        FTD_Opened_Within_14_Days_Maturity,
        case
            when b.Interest_Accrued_Amount is not null then b.Interest_Accrued_Amount
            else a.Interest_Accrued_Amount
        end as Interest_Accrued_Amount,
        c.FTD_Balance_at_Maturity,
        c.FTD_Balance_at_Maturity_in_GBP,
        case
            when d.account_state = 'ACTIVE' then 1
            else 0
        end as active_flag,
        row_number() over(
            partition by a.user_id,
            a.Currency_Code
            order by
                a.approved_date
        ) as rn_first_currency,
        row_number() over(
            partition by a.user_id,
            a.Currency_Code
            order by
                a.approved_date desc
        ) as rn_last_currency
    from
        label_maturity_date a
        left join matured_ftd_interest b on a.account_id = b.account_id
        left join matured_ftd_amount c on a.account_id = c.account_id
        left join latest_account_state d on a.account_id = d.account_id
),
per_customer_level as(
    select
        distinct Customer_ID,
        count(
            case
                when Currency_Code = 'GBP' then FTD_Account_ID
                else null
            end
        ) over (partition by Customer_ID) as Number_GBP_FTDs,
        count(
            case
                when Currency_Code = 'EUR' then FTD_Account_ID
                else null
            end
        ) over (partition by Customer_ID) as Number_EUR_FTDs,
        count(
            case
                when Currency_Code = 'USD' then FTD_Account_ID
                else null
            end
        ) over (partition by Customer_ID) as Number_USD_FTDs,
        sum(
            case
                when Currency_Code = 'GBP' then FTD_Balance
                else null
            end
        ) over (partition by Customer_ID) as FTD_GBP_Total_Balance,
        sum(
            case
                when Currency_Code = 'EUR' then FTD_Balance
                else null
            end
        ) over (partition by Customer_ID) as FTD_EUR_Total_Balance,
        sum(
            case
                when Currency_Code = 'USD' then FTD_Balance
                else null
            end
        ) over (partition by Customer_ID) as FTD_USD_Total_Balance,
        sum(
            case
                when Currency_Code = 'GBP'
                and active_flag = 1 then FTD_Balance
                else null
            end
        ) over (partition by Customer_ID) as FTD_GBP_Cur_Balance,
        sum(
            case
                when Currency_Code = 'EUR'
                and active_flag = 1 then FTD_Balance
                else null
            end
        ) over (partition by Customer_ID) as FTD_EUR_Cur_Balance,
        sum(
            case
                when Currency_Code = 'USD'
                and active_flag = 1 then FTD_Balance
                else null
            end
        ) over (partition by Customer_ID) as FTD_USD_Cur_Balance,
        MAX(
            case
                when Currency_Code = 'GBP'
                and active_flag = 1 THEN TRUE
                ELSE FALSE
            END
        ) over (partition by Customer_ID) AS FTD_GBP_Is_Currently_Open,
        MAX(
            case
                when Currency_Code = 'EUR'
                and active_flag = 1 THEN TRUE
                ELSE FALSE
            END
        ) over (partition by Customer_ID) AS FTD_EUR_Is_Currently_Open,
        MAX(
            case
                when Currency_Code = 'USD'
                and active_flag = 1 THEN TRUE
                ELSE FALSE
            END
        ) over (partition by Customer_ID) AS FTD_USD_Is_Currently_Open,
        MAX(
            case
                when Currency_Code = 'GBP' THEN TRUE
                ELSE FALSE
            END
        ) over (partition by Customer_ID) AS FTD_GBP_Has_Open,
        MAX(
            case
                when Currency_Code = 'EUR' THEN TRUE
                ELSE FALSE
            END
        ) over (partition by Customer_ID) AS FTD_EUR_Has_Open,
        MAX(
            case
                when Currency_Code = 'USD' THEN TRUE
                ELSE FALSE
            END
        ) over (partition by Customer_ID) AS FTD_USD_Has_Open,
        MAX(
            case
                when Currency_Code = 'GBP'
                and rn_first_currency = 1 then FTD_Open_Date
            end
        ) over (partition by Customer_ID) as FTD_GBP_First_Inbound_Date,
        MAX(
            case
                when Currency_Code = 'EUR'
                and rn_first_currency = 1 then FTD_Open_Date
            end
        ) over (partition by Customer_ID) as FTD_EUR_First_Inbound_Date,
        MAX(
            case
                when Currency_Code = 'USD'
                and rn_first_currency = 1 then FTD_Open_Date
            end
        ) over (partition by Customer_ID) as FTD_USD_First_Inbound_Date,
        MAX(
            case
                when Currency_Code = 'GBP'
                and rn_last_currency = 1 then FTD_Open_Date
            end
        ) over (partition by Customer_ID) as FTD_GBP_Last_Inbound_Date,
        MAX(
            case
                when Currency_Code = 'EUR'
                and rn_last_currency = 1 then FTD_Open_Date
            end
        ) over (partition by Customer_ID) as FTD_EUR_Last_Inbound_Date,
        MAX(
            case
                when Currency_Code = 'USD'
                and rn_last_currency = 1 then FTD_Open_Date
            end
        ) over (partition by Customer_ID) as FTD_USD_Last_Inbound_Date
    from
        per_account_level
),
final_tb_1 as (
    SELECT
        distinct a.Customer_ID as FTD_Customer_ID,
        a.Account_Holder_Key as FTD_Account_Holder_key,
        b.Affluency as FTD_Affluency,
        c.brand_id as FTD_Brand_ID,
        c.residence_country_code AS FTD_Residence_Country_Code,
        c.customer_status as FTD_Customer_Status,
        FTD_Account_ID,
        Account_Type as FTD_Account_Type,
        Name as FTD_Name,
        d.account_state as FTD_Account_State,
        FTD_Term,
        cast(FTD_Interest_Rate as decimal(18, 2)) as FTD_Interest_Rate,
        Currency_Code as FTD_Currency_Code,
        FTD_Balance,
        FTD_Balance_in_GBP,
        cast(Interest_Accrued_Amount as decimal(18, 2)) as FTD_Interest_Accrued_Amount,
        FTD_Open_Date,
        FTD_Maturity_Date,
        DATE(FTD_Maturity_Date_plus_1hr) AS FTD_Maturity_Date_plus_1hr,
        cast(FTD_Balance_at_Maturity as decimal(18, 2)) as FTD_Balance_at_Maturity,
        cast(FTD_Balance_at_Maturity_in_GBP as decimal(18, 2)) as FTD_Balance_at_Maturity_in_GBP,
        FTD_Next_Account_ID,
        FTD_Next_Approved_Date,
        COALESCE(FTD_Opened_Within_1_Days_Maturity, 0) as FTD_Opened_Within_1_Days_Maturity,
        COALESCE(FTD_Opened_Within_3_Days_Maturity, 0) as FTD_Opened_Within_3_Days_Maturity,
        COALESCE(FTD_Opened_Within_7_Days_Maturity, 0) as FTD_Opened_Within_7_Days_Maturity,
        COALESCE(FTD_Opened_Within_14_Days_Maturity, 0) as FTD_Opened_Within_14_Days_Maturity,
        Number_GBP_FTDs as FTD_Number_GBP_FTDs,
        cast(FTD_GBP_Total_Balance as decimal(18, 2)) as FTD_GBP_Total_Balance,
        FTD_GBP_Cur_Balance,
        FTD_GBP_Is_Currently_Open,
        FTD_GBP_Has_Open,
        FTD_GBP_First_Inbound_Date,
        FTD_GBP_Last_Inbound_Date,
        Number_EUR_FTDs as FTD_Number_EUR_FTDs,
        cast(FTD_EUR_Total_Balance as decimal(18, 2)) as FTD_EUR_Total_Balance,
        FTD_EUR_Cur_Balance,
        FTD_EUR_Is_Currently_Open,
        FTD_EUR_Has_Open,
        FTD_EUR_First_Inbound_Date,
        FTD_EUR_Last_Inbound_Date,
        Number_USD_FTDs as FTD_Number_USD_FTDs,
        cast(FTD_USD_Total_Balance as decimal(18, 2)) as FTD_USD_Total_Balance,
        FTD_USD_Cur_Balance,
        FTD_USD_Is_Currently_Open,
        FTD_USD_Has_Open,
        FTD_USD_First_Inbound_Date,
        FTD_USD_Last_Inbound_Date
    FROM
        per_account_level a
        left join (
            select
                distinct user_id,
                Affluency
            from
                (
                    with risk_form as (
                        select
                            *
                        from
                            (
                                select
                                    dynamodb_new_image_customer_id_s,
                                    dynamodb_new_image_form_data_m_customer_occupations_l_0_s,
                                    dynamodb_new_image_form_data_m_customer_occupations_l_1_s,
                                    dynamodb_new_image_form_data_m_customer_occupations_l_2_s,
                                    dynamodb_new_image_form_data_m_customer_occupations_l_3_s,
                                    dynamodb_new_image_form_data_m_customer_occupations_l_4_s,
                                    dynamodb_new_image_form_data_m_customer_circumstances_l_0_s,
                                    dynamodb_new_image_form_data_m_customer_circumstances_l_1_s,
                                    dynamodb_new_image_form_data_m_customer_circumstances_l_2_s,
                                    row_number() over(
                                        partition by dynamodb_new_image_customer_id_s
                                        order by
                                            dynamodb_new_image_updated_at_n desc
                                    ) rn
                                from
                                    datalake_curated.dynamo_sls_customer_risk_form
                            ) as a
                        where
                            rn = 1
                    ),
                    occupation_tb as(
                        select
                            *,
                            count(occupations) over(partition by dynamodb_new_image_customer_id_s) as occu_count
                        from
                            (
                                select
                                    distinct dynamodb_new_image_customer_id_s,
                                    nullif(ltrim(rtrim(nullif(occupations, 'nan'))), '') as occupations
                                from
                                    risk_form r
                                    cross join unnest(
                                        array [dynamodb_new_image_form_data_m_customer_occupations_l_0_s,
    dynamodb_new_image_form_data_m_customer_occupations_l_1_s,
    dynamodb_new_image_form_data_m_customer_occupations_l_2_s,
    dynamodb_new_image_form_data_m_customer_occupations_l_3_s,
    dynamodb_new_image_form_data_m_customer_occupations_l_4_s]
                                    ) t(occupations)
                                where
                                    occupations is not null
                            ) a
                        where
                            occupations is not null
                    ),
                    circumstance_tb as(
                        select
                            *,
                            count(circumstance) over(partition by dynamodb_new_image_customer_id_s) as circum_count
                        from
                            (
                                select
                                    distinct dynamodb_new_image_customer_id_s,
                                    nullif(ltrim(rtrim(nullif(circumstance, 'nan'))), '') as circumstance
                                from
                                    risk_form r
                                    cross join unnest(
                                        array [
    dynamodb_new_image_form_data_m_customer_circumstances_l_0_s,
    dynamodb_new_image_form_data_m_customer_circumstances_l_1_s,
    dynamodb_new_image_form_data_m_customer_circumstances_l_2_s
    ]
                                    ) t(circumstance)
                                where
                                    circumstance is not null
                            ) a
                        where
                            circumstance is not null
                    )
                    select
                        rg.*,
                        dynamodb_individual_m_nationality_country_code_s as Nationality,
                        age_range,
                        age,
                        case
                            when brand_id is null
                            or ltrim(rtrim(brand_id)) = '' then 'NOMO_BANK'
                            else brand_id
                        end as brand_id,
                        case
                            when dynamodb_new_gid = '1' then 'Female'
                            when dynamodb_new_gid = '0' then 'Male'
                        end as gender,
                        --sr.income_wealth_source_count,
                        CASE
                            WHEN Income_GBP_Value IS NULL THEN NULL
                            WHEN Income_GBP_Value <= 5000 then 'Less than £5000 pa'
                            WHEN Income_GBP_Value <= 10000 then '£5000 to £10,000 pa'
                            WHEN Income_GBP_Value <= 50000 then '£10,000 to £50,000 pa'
                            WHEN Income_GBP_Value <= 100000 then '£50,000 to £100,000 pa'
                            WHEN Income_GBP_Value <= 500000 then '£100,000 to £500,000 pa'
                            ELSE 'Over £500,000pa'
                        END as income_bracket,
                        CASE
                            WHEN Assets_GBP_Value IS NULL THEN NULL
                            WHEN Assets_GBP_Value <= 5000 then 'Less than £5000'
                            WHEN Assets_GBP_Value <= 10000 then '£5000 to £10,000'
                            WHEN Assets_GBP_Value <= 50000 then '£10,000 to £50,000'
                            WHEN Assets_GBP_Value <= 100000 then '£50,000 to £100,000'
                            WHEN Assets_GBP_Value <= 500000 then '£100,000 to £500,000'
                            WHEN Assets_GBP_Value <= 1000000 then '£500,000 to £1,000,000'
                            ELSE 'Over £1,000,000'
                        END as Wealth_bracket,
                        CASE
                            WHEN Income_GBP_Value IS NULL
                            and Assets_GBP_Value IS NULL then NULL
                            WHEN Income_GBP_Value > 0
                            and Assets_GBP_Value > 0 then 'Income and Wealth'
                            WHEN Income_GBP_Value > 0
                            and (
                                Assets_GBP_Value IS NULL
                                or Assets_GBP_Value = 0
                            ) then 'Income'
                            WHEN (
                                Income_GBP_Value IS NULL
                                or Income_GBP_Value = 0
                            )
                            and Assets_GBP_Value > 0 then 'Wealth'
                            ELSE NULL
                        END AS income_wealth_type,
                        occ.occupations,
                        occ.occu_count,
                        cir.circumstance,
                        cir.circum_count,
                        sr.income_wealth_value,
                        sr.income_wealth_source_count
                    from
                        datalake_curated.customer_timeline_detail a
                        left join (
                            select
                                *
                            from
                                (
                                    with customer_table_income as (
                                        --- Monthly Income Formula = ( {(a + b)/2} * 12 ) * Exchange_Rate
                                        --- Updated Exchange Rates (TradeCo defined) as follows:
                                        -- USD = 0.80
                                        -- EUR = 0.86
                                        -- AED = 0.22
                                        -- SAR = 0.22
                                        -- KWD = 2.65
                                        select
                                            COALESCE(
                                                dynamodb_keys_id_s,
                                                dynamodb_key_id_s,
                                                dynamodb_new_image_id_s
                                            ) as user_id,
                                            case
                                                when dynamodb_new_image_individual_m_financial_details_m_monthly_income_s = '0_2000_SAR_MONTH' then 2600
                                                when dynamodb_new_image_individual_m_financial_details_m_monthly_income_s = 'AED_LESS_THAN_2000' then 2600
                                                when dynamodb_new_image_individual_m_financial_details_m_monthly_income_s = 'GBP_3000_TO_4000' then 42000
                                                when dynamodb_new_image_individual_m_financial_details_m_monthly_income_s = 'KWD_LESS_THAN_1000' then 15900
                                                when dynamodb_new_image_individual_m_financial_details_m_monthly_income_s = 'GBP_LESS_THAN_1000' then 6000
                                                when dynamodb_new_image_individual_m_financial_details_m_monthly_income_s = '6000_12000_GBP_MONTH' then 108000
                                                when dynamodb_new_image_individual_m_financial_details_m_monthly_income_s = '0_1000_GBP_MONTH' then 6000
                                                when dynamodb_new_image_individual_m_financial_details_m_monthly_income_s = '3000_4000_GBP_MONTH' then 42000
                                                when dynamodb_new_image_individual_m_financial_details_m_monthly_income_s = 'KWD_1000_TO_3999' then 79400
                                                when dynamodb_new_image_individual_m_financial_details_m_monthly_income_s = '' then null
                                                when dynamodb_new_image_individual_m_financial_details_m_monthly_income_s = '1000_3000_GBP_MONTH' then 24000
                                                when dynamodb_new_image_individual_m_financial_details_m_monthly_income_s = 'GBP_6000_TO_12000' then 108000
                                                when dynamodb_new_image_individual_m_financial_details_m_monthly_income_s = 'GBP_4000_TO_6000' then 60000
                                                when dynamodb_new_image_individual_m_financial_details_m_monthly_income_s = 'MORE_THAN_7000_KWD_MONTH' then 222600
                                                when dynamodb_new_image_individual_m_financial_details_m_monthly_income_s = 'KWD_4000_TO_7000' then 174900
                                                when dynamodb_new_image_individual_m_financial_details_m_monthly_income_s = 'KWD_MORE_THAN_7000' then 222600
                                                when dynamodb_new_image_individual_m_financial_details_m_monthly_income_s = 'GBP_MORE_THAN_12000' then 144000
                                                when dynamodb_new_image_individual_m_financial_details_m_monthly_income_s = '1000_3999_KWD_MONTH' then 79400
                                                when dynamodb_new_image_individual_m_financial_details_m_monthly_income_s = '4000_6000_GBP_MONTH' then 60000
                                                when dynamodb_new_image_individual_m_financial_details_m_monthly_income_s = 'MORE_THAN_12000_GBP_MONTH' then 144000
                                                when dynamodb_new_image_individual_m_financial_details_m_monthly_income_s = '0_1000_KWD_MONTH' then 15900
                                                when dynamodb_new_image_individual_m_financial_details_m_monthly_income_s = '4000_7000_KWD_MONTH' then 174900
                                                when dynamodb_new_image_individual_m_financial_details_m_monthly_income_s = '0_20000_AED_MONTH' then 26400
                                                when dynamodb_new_image_individual_m_financial_details_m_monthly_income_s = 'GBP_1000_TO_3000' then 24000
                                                when dynamodb_new_image_individual_m_financial_details_m_monthly_income_s = '55000_85000_AED_MONTH' then 184800
                                                when dynamodb_new_image_individual_m_financial_details_m_monthly_income_s = '3000_4999_SAR_MONTH' then 10500
                                                when dynamodb_new_image_individual_m_financial_details_m_monthly_income_s = 'MORE_THAN_125000_AED_MONTH' then 330000
                                                when dynamodb_new_image_individual_m_financial_details_m_monthly_income_s = 'MORE_THAN_15000_SAR_MONTH' then 39600
                                                when dynamodb_new_image_individual_m_financial_details_m_monthly_income_s = 'AED_MORE_THAN_10000' then 26400
                                                when dynamodb_new_image_individual_m_financial_details_m_monthly_income_s = 'AED_5000_TO_7000' then 15800
                                                when dynamodb_new_image_individual_m_financial_details_m_monthly_income_s = 'AED_3000_TO_5000' then 10500
                                                when dynamodb_new_image_individual_m_financial_details_m_monthly_income_s = 'AED_7000_TO_10000' then 22400
                                                when dynamodb_new_image_individual_m_financial_details_m_monthly_income_s = '20000_35000_AED_MONTH' then 72600
                                                when dynamodb_new_image_individual_m_financial_details_m_monthly_income_s = '35000_55000_AED_MONTH' then 118800
                                                when dynamodb_new_image_individual_m_financial_details_m_monthly_income_s = '8000_14999_SAR_MONTH' then 30300
                                                when dynamodb_new_image_individual_m_financial_details_m_monthly_income_s = '5000_7999_SAR_MONTH' then 17100
                                                when dynamodb_new_image_individual_m_financial_details_m_monthly_income_s = 'AED_2000_TO_3000' then 6600
                                                when dynamodb_new_image_individual_m_financial_details_m_monthly_income_s = '85000_125000_AED_MONTH' then 277000
                                                when dynamodb_new_image_individual_m_financial_details_m_monthly_income_s = '2000_2999_SAR_MONTH' then 6500
                                                when dynamodb_new_image_individual_m_financial_details_m_monthly_income_s = 'MORE_THAN_100000_AED_MONTH' then 264000
                                                when dynamodb_new_image_individual_m_financial_details_m_monthly_income_s = '10000_14999_AED_MONTH' then 32900
                                                when dynamodb_new_image_individual_m_financial_details_m_monthly_income_s = '40000_99999_AED_MONTH' then 184700
                                                when dynamodb_new_image_individual_m_financial_details_m_monthly_income_s = '15000_19999_AED_MONTH' then 46100
                                                when dynamodb_new_image_individual_m_financial_details_m_monthly_income_s = '20000_39999_AED_MONTH' then 79100
                                                when dynamodb_new_image_individual_m_financial_details_m_monthly_income_s = '0_9999_AED_MONTH' then 13100
                                                ELSE null
                                            end as Income_GBP_Cleaned,
                                            case
                                                when dynamodb_new_image_individual_m_financial_details_m_estimated_assets_value_s = '0_30000_KWD' then 39700
                                                when dynamodb_new_image_individual_m_financial_details_m_estimated_assets_value_s = '85000_199000_GBP' then 142000
                                                when dynamodb_new_image_individual_m_financial_details_m_estimated_assets_value_s = '30000_80000_KWD' then 145700
                                                when dynamodb_new_image_individual_m_financial_details_m_estimated_assets_value_s = '0_50000_SAR' then 5500
                                                when dynamodb_new_image_individual_m_financial_details_m_estimated_assets_value_s = 'KWD_150K_TO_300K' then 596200
                                                when dynamodb_new_image_individual_m_financial_details_m_estimated_assets_value_s = 'KWD_80K_TO_150K' then 304700
                                                when dynamodb_new_image_individual_m_financial_details_m_estimated_assets_value_s = '20000_84000_GBP' then 52000
                                                when dynamodb_new_image_individual_m_financial_details_m_estimated_assets_value_s = 'GBP_LESS_THAN_20K' then 10000
                                                when dynamodb_new_image_individual_m_financial_details_m_estimated_assets_value_s = 'GBP_20K_TO_84K' then 52000
                                                when dynamodb_new_image_individual_m_financial_details_m_estimated_assets_value_s = 'MORE_THAN_300000_KWD' then 795000
                                                when dynamodb_new_image_individual_m_financial_details_m_estimated_assets_value_s = '150000_300000_KWD' then 596200
                                                when dynamodb_new_image_individual_m_financial_details_m_estimated_assets_value_s = 'GBP_200K_TO_299K' then 249500
                                                when dynamodb_new_image_individual_m_financial_details_m_estimated_assets_value_s = 'GBP_85K_TO_199K' then 142000
                                                when dynamodb_new_image_individual_m_financial_details_m_estimated_assets_value_s = 'MORE_THAN_1000000_GBP' then 1000000
                                                when dynamodb_new_image_individual_m_financial_details_m_estimated_assets_value_s = 'KWD_MORE_THAN_300K' then 795000
                                                when dynamodb_new_image_individual_m_financial_details_m_estimated_assets_value_s = '0_20000_GBP' then 10000
                                                when dynamodb_new_image_individual_m_financial_details_m_estimated_assets_value_s = 'KWD_LESS_THAN_30K' then 39700
                                                when dynamodb_new_image_individual_m_financial_details_m_estimated_assets_value_s = 'AED_LESS_THAN_35K' then 3800
                                                when dynamodb_new_image_individual_m_financial_details_m_estimated_assets_value_s = '200000_299000_GBP' then 249500
                                                when dynamodb_new_image_individual_m_financial_details_m_estimated_assets_value_s = '0_35000_AED' then 3800
                                                when dynamodb_new_image_individual_m_financial_details_m_estimated_assets_value_s = '' then null
                                                when dynamodb_new_image_individual_m_financial_details_m_estimated_assets_value_s = 'GBP_MORE_THAN_1M' then 1000000
                                                when dynamodb_new_image_individual_m_financial_details_m_estimated_assets_value_s = '500000_1000000_GBP' then 750000
                                                when dynamodb_new_image_individual_m_financial_details_m_estimated_assets_value_s = 'KWD_30K_TO_80K' then 140000
                                                when dynamodb_new_image_individual_m_financial_details_m_estimated_assets_value_s = 'GBP_500K_TO_1M' then 750000
                                                when dynamodb_new_image_individual_m_financial_details_m_estimated_assets_value_s = '80000_150000_KWD' then 304700
                                                when dynamodb_new_image_individual_m_financial_details_m_estimated_assets_value_s = 'AED_500K_TO_1_5M' then 220000
                                                when dynamodb_new_image_individual_m_financial_details_m_estimated_assets_value_s = 'AED_35K_TO_49K' then 9200
                                                when dynamodb_new_image_individual_m_financial_details_m_estimated_assets_value_s = 'AED_50K_TO_99K' then 16300
                                                when dynamodb_new_image_individual_m_financial_details_m_estimated_assets_value_s = 'AED_100K_TO_499K' then 65800
                                                when dynamodb_new_image_individual_m_financial_details_m_estimated_assets_value_s = 'AED_MORE_THAN_1_5M' then 330000
                                                when dynamodb_new_image_individual_m_financial_details_m_estimated_assets_value_s = '35000_49000_AED' then 9240
                                                when dynamodb_new_image_individual_m_financial_details_m_estimated_assets_value_s = '500000_1500000_AED' then 220000
                                                when dynamodb_new_image_individual_m_financial_details_m_estimated_assets_value_s = '50000_99000_AED' then 16300
                                                when dynamodb_new_image_individual_m_financial_details_m_estimated_assets_value_s = '750000_1000000_SAR' then 192500
                                                when dynamodb_new_image_individual_m_financial_details_m_estimated_assets_value_s = '100000_499000_AED' then 65800
                                                when dynamodb_new_image_individual_m_financial_details_m_estimated_assets_value_s = 'MORE_THAN_1500000_AED' then 330000
                                                when dynamodb_new_image_individual_m_financial_details_m_estimated_assets_value_s = '250000_750000_SAR' then 110000
                                                when dynamodb_new_image_individual_m_financial_details_m_estimated_assets_value_s = 'MORE_THAN_1000000_SAR' then 220000
                                                when dynamodb_new_image_individual_m_financial_details_m_estimated_assets_value_s = '100000_250000_SAR' then 38500
                                                when dynamodb_new_image_individual_m_financial_details_m_estimated_assets_value_s = '50000_100000_SAR' then 16500
                                                ELSE null
                                            END as Assets_GBP_Cleaned,
                                            dynamodb_new_image_individual_m_address_m_country_code_s,
                                            dynamodb_new_image_status_s,
                                            dynamodb_new_image_card_ordered_bool,
                                            row_number() over(
                                                partition by COALESCE(dynamodb_keys_id_s, dynamodb_key_id_s)
                                                order by
                                                    dynamodb_new_image_updated_at_n desc,
                                                    dynamodb_approximate_creation_date_time desc
                                            ) as rn_last
                                        from
                                            datalake_curated.dynamo_scv_sls_customers a
                                    ),
                                    --select * from customer_table_income
                                    income_wealth_wide as (
                                        select
                                            dynamodb_keys_customer_id_s,
                                            kv1 ['income'] as income,
                                            kv1 ['wealth'] as wealth,
                                            kv1 ['average_net_worth'] as average_net_worth
                                        From
                                            (
                                                select
                                                    dynamodb_keys_customer_id_s,
                                                    map_agg(income_wealth_type, income_wealth_sum_of_average) kv1
                                                from
                                                    (
                                                        with get_distinct as (
                                                            select
                                                                *,
                                                                (
                                                                    (
                                                                        converted_estimated_net_worth_max_local + converted_estimated_net_worth_min_local
                                                                    ) / 2
                                                                ) as net_worth
                                                            from
                                                                (
                                                                    select
                                                                        distinct dynamodb_keys_customer_id_s,
                                                                        case
                                                                            when items_new_0_type in (
                                                                                'INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS',
                                                                                'INCOME_FROM_PERSONAL_SAVINGS',
                                                                                'SAVINGS_FROM_SALARY_OR_EARNINGS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_0_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_0_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as income_0,
                                                                        case
                                                                            when items_new_1_type in (
                                                                                'INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS',
                                                                                'INCOME_FROM_PERSONAL_SAVINGS',
                                                                                'SAVINGS_FROM_SALARY_OR_EARNINGS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_1_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_1_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as income_1,
                                                                        case
                                                                            when items_new_5_type in (
                                                                                'INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS',
                                                                                'INCOME_FROM_PERSONAL_SAVINGS',
                                                                                'SAVINGS_FROM_SALARY_OR_EARNINGS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_5_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_5_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as income_5,
                                                                        case
                                                                            when items_new_27_type in (
                                                                                'INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS',
                                                                                'INCOME_FROM_PERSONAL_SAVINGS',
                                                                                'SAVINGS_FROM_SALARY_OR_EARNINGS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_27_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_27_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as income_27,
                                                                        case
                                                                            when items_new_2_type in (
                                                                                'INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS',
                                                                                'INCOME_FROM_PERSONAL_SAVINGS',
                                                                                'SAVINGS_FROM_SALARY_OR_EARNINGS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_2_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_2_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as income_2,
                                                                        case
                                                                            when items_new_4_type in (
                                                                                'INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS',
                                                                                'INCOME_FROM_PERSONAL_SAVINGS',
                                                                                'SAVINGS_FROM_SALARY_OR_EARNINGS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_4_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_4_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as income_4,
                                                                        case
                                                                            when items_new_20_type in (
                                                                                'INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS',
                                                                                'INCOME_FROM_PERSONAL_SAVINGS',
                                                                                'SAVINGS_FROM_SALARY_OR_EARNINGS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_20_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_20_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as income_20,
                                                                        case
                                                                            when items_new_10_type in (
                                                                                'INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS',
                                                                                'INCOME_FROM_PERSONAL_SAVINGS',
                                                                                'SAVINGS_FROM_SALARY_OR_EARNINGS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_10_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_10_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as income_10,
                                                                        case
                                                                            when items_new_12_type in (
                                                                                'INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS',
                                                                                'INCOME_FROM_PERSONAL_SAVINGS',
                                                                                'SAVINGS_FROM_SALARY_OR_EARNINGS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_12_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_12_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as income_12,
                                                                        case
                                                                            when items_new_14_type in (
                                                                                'INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS',
                                                                                'INCOME_FROM_PERSONAL_SAVINGS',
                                                                                'SAVINGS_FROM_SALARY_OR_EARNINGS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_14_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_14_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as income_14,
                                                                        case
                                                                            when items_new_16_type in (
                                                                                'INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS',
                                                                                'INCOME_FROM_PERSONAL_SAVINGS',
                                                                                'SAVINGS_FROM_SALARY_OR_EARNINGS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_16_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_16_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as income_16,
                                                                        case
                                                                            when items_new_9_type in (
                                                                                'INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS',
                                                                                'INCOME_FROM_PERSONAL_SAVINGS',
                                                                                'SAVINGS_FROM_SALARY_OR_EARNINGS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_9_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_9_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as income_9,
                                                                        case
                                                                            when items_new_17_type in (
                                                                                'INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS',
                                                                                'INCOME_FROM_PERSONAL_SAVINGS',
                                                                                'SAVINGS_FROM_SALARY_OR_EARNINGS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_17_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_17_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as income_17,
                                                                        case
                                                                            when items_new_23_type in (
                                                                                'INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS',
                                                                                'INCOME_FROM_PERSONAL_SAVINGS',
                                                                                'SAVINGS_FROM_SALARY_OR_EARNINGS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_23_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_23_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as income_23,
                                                                        case
                                                                            when items_new_26_type in (
                                                                                'INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS',
                                                                                'INCOME_FROM_PERSONAL_SAVINGS',
                                                                                'SAVINGS_FROM_SALARY_OR_EARNINGS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_26_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_26_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as income_26,
                                                                        case
                                                                            when items_new_7_type in (
                                                                                'INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS',
                                                                                'INCOME_FROM_PERSONAL_SAVINGS',
                                                                                'SAVINGS_FROM_SALARY_OR_EARNINGS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_7_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_7_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as income_7,
                                                                        case
                                                                            when items_new_8_type in (
                                                                                'INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS',
                                                                                'INCOME_FROM_PERSONAL_SAVINGS',
                                                                                'SAVINGS_FROM_SALARY_OR_EARNINGS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_8_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_8_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as income_8,
                                                                        case
                                                                            when items_new_21_type in (
                                                                                'INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS',
                                                                                'INCOME_FROM_PERSONAL_SAVINGS',
                                                                                'SAVINGS_FROM_SALARY_OR_EARNINGS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_21_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_21_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as income_21,
                                                                        case
                                                                            when items_new_22_type in (
                                                                                'INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS',
                                                                                'INCOME_FROM_PERSONAL_SAVINGS',
                                                                                'SAVINGS_FROM_SALARY_OR_EARNINGS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_22_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_22_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as income_22,
                                                                        case
                                                                            when items_new_24_type in (
                                                                                'INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS',
                                                                                'INCOME_FROM_PERSONAL_SAVINGS',
                                                                                'SAVINGS_FROM_SALARY_OR_EARNINGS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_24_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_24_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as income_24,
                                                                        case
                                                                            when items_new_3_type in (
                                                                                'INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS',
                                                                                'INCOME_FROM_PERSONAL_SAVINGS',
                                                                                'SAVINGS_FROM_SALARY_OR_EARNINGS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_3_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_3_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as income_3,
                                                                        case
                                                                            when items_new_11_type in (
                                                                                'INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS',
                                                                                'INCOME_FROM_PERSONAL_SAVINGS',
                                                                                'SAVINGS_FROM_SALARY_OR_EARNINGS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_11_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_11_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as income_11,
                                                                        case
                                                                            when items_new_13_type in (
                                                                                'INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS',
                                                                                'INCOME_FROM_PERSONAL_SAVINGS',
                                                                                'SAVINGS_FROM_SALARY_OR_EARNINGS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_13_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_13_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as income_13,
                                                                        case
                                                                            when items_new_19_type in (
                                                                                'INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS',
                                                                                'INCOME_FROM_PERSONAL_SAVINGS',
                                                                                'SAVINGS_FROM_SALARY_OR_EARNINGS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_19_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_19_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as income_19,
                                                                        case
                                                                            when items_new_25_type in (
                                                                                'INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS',
                                                                                'INCOME_FROM_PERSONAL_SAVINGS',
                                                                                'SAVINGS_FROM_SALARY_OR_EARNINGS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_25_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_25_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as income_25,
                                                                        case
                                                                            when items_new_29_type in (
                                                                                'INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS',
                                                                                'INCOME_FROM_PERSONAL_SAVINGS',
                                                                                'SAVINGS_FROM_SALARY_OR_EARNINGS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_29_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_29_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as income_29,
                                                                        case
                                                                            when items_new_6_type in (
                                                                                'INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS',
                                                                                'INCOME_FROM_PERSONAL_SAVINGS',
                                                                                'SAVINGS_FROM_SALARY_OR_EARNINGS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_6_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_6_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as income_6,
                                                                        case
                                                                            when items_new_15_type in (
                                                                                'INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS',
                                                                                'INCOME_FROM_PERSONAL_SAVINGS',
                                                                                'SAVINGS_FROM_SALARY_OR_EARNINGS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_15_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_15_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as income_15,
                                                                        case
                                                                            when items_new_18_type in (
                                                                                'INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS',
                                                                                'INCOME_FROM_PERSONAL_SAVINGS',
                                                                                'SAVINGS_FROM_SALARY_OR_EARNINGS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_18_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_18_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as income_18,
                                                                        case
                                                                            when items_new_28_type in (
                                                                                'INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS',
                                                                                'INCOME_FROM_PERSONAL_SAVINGS',
                                                                                'SAVINGS_FROM_SALARY_OR_EARNINGS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_28_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_28_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as income_28,
                                                                        case
                                                                            when items_new_0_type in (
                                                                                'BUSINESS_PROFITS_DIVIDENDS_OR_SALES',
                                                                                'STOCKS_SHARES_AND_FUNDS',
                                                                                'OTHER',
                                                                                'INHERITANCE_OR_GIFTS',
                                                                                'PREMIUM_ASSETS',
                                                                                'PROPERTY_SALES',
                                                                                'VIRTUAL_ASSETS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_0_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_0_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as wealth_0,
                                                                        case
                                                                            when items_new_1_type in (
                                                                                'BUSINESS_PROFITS_DIVIDENDS_OR_SALES',
                                                                                'STOCKS_SHARES_AND_FUNDS',
                                                                                'OTHER',
                                                                                'INHERITANCE_OR_GIFTS',
                                                                                'PREMIUM_ASSETS',
                                                                                'PROPERTY_SALES',
                                                                                'VIRTUAL_ASSETS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_1_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_1_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as wealth_1,
                                                                        case
                                                                            when items_new_5_type in (
                                                                                'BUSINESS_PROFITS_DIVIDENDS_OR_SALES',
                                                                                'STOCKS_SHARES_AND_FUNDS',
                                                                                'OTHER',
                                                                                'INHERITANCE_OR_GIFTS',
                                                                                'PREMIUM_ASSETS',
                                                                                'PROPERTY_SALES',
                                                                                'VIRTUAL_ASSETS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_5_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_5_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as wealth_5,
                                                                        case
                                                                            when items_new_27_type in (
                                                                                'BUSINESS_PROFITS_DIVIDENDS_OR_SALES',
                                                                                'STOCKS_SHARES_AND_FUNDS',
                                                                                'OTHER',
                                                                                'INHERITANCE_OR_GIFTS',
                                                                                'PREMIUM_ASSETS',
                                                                                'PROPERTY_SALES',
                                                                                'VIRTUAL_ASSETS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_27_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_27_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as wealth_27,
                                                                        case
                                                                            when items_new_2_type in (
                                                                                'BUSINESS_PROFITS_DIVIDENDS_OR_SALES',
                                                                                'STOCKS_SHARES_AND_FUNDS',
                                                                                'OTHER',
                                                                                'INHERITANCE_OR_GIFTS',
                                                                                'PREMIUM_ASSETS',
                                                                                'PROPERTY_SALES',
                                                                                'VIRTUAL_ASSETS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_2_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_2_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as wealth_2,
                                                                        case
                                                                            when items_new_4_type in (
                                                                                'BUSINESS_PROFITS_DIVIDENDS_OR_SALES',
                                                                                'STOCKS_SHARES_AND_FUNDS',
                                                                                'OTHER',
                                                                                'INHERITANCE_OR_GIFTS',
                                                                                'PREMIUM_ASSETS',
                                                                                'PROPERTY_SALES',
                                                                                'VIRTUAL_ASSETS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_4_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_4_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as wealth_4,
                                                                        case
                                                                            when items_new_20_type in (
                                                                                'BUSINESS_PROFITS_DIVIDENDS_OR_SALES',
                                                                                'STOCKS_SHARES_AND_FUNDS',
                                                                                'OTHER',
                                                                                'INHERITANCE_OR_GIFTS',
                                                                                'PREMIUM_ASSETS',
                                                                                'PROPERTY_SALES',
                                                                                'VIRTUAL_ASSETS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_20_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_20_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as wealth_20,
                                                                        case
                                                                            when items_new_10_type in (
                                                                                'BUSINESS_PROFITS_DIVIDENDS_OR_SALES',
                                                                                'STOCKS_SHARES_AND_FUNDS',
                                                                                'OTHER',
                                                                                'INHERITANCE_OR_GIFTS',
                                                                                'PREMIUM_ASSETS',
                                                                                'PROPERTY_SALES',
                                                                                'VIRTUAL_ASSETS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_10_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_10_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as wealth_10,
                                                                        case
                                                                            when items_new_12_type in (
                                                                                'BUSINESS_PROFITS_DIVIDENDS_OR_SALES',
                                                                                'STOCKS_SHARES_AND_FUNDS',
                                                                                'OTHER',
                                                                                'INHERITANCE_OR_GIFTS',
                                                                                'PREMIUM_ASSETS',
                                                                                'PROPERTY_SALES',
                                                                                'VIRTUAL_ASSETS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_12_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_12_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as wealth_12,
                                                                        case
                                                                            when items_new_14_type in (
                                                                                'BUSINESS_PROFITS_DIVIDENDS_OR_SALES',
                                                                                'STOCKS_SHARES_AND_FUNDS',
                                                                                'OTHER',
                                                                                'INHERITANCE_OR_GIFTS',
                                                                                'PREMIUM_ASSETS',
                                                                                'PROPERTY_SALES',
                                                                                'VIRTUAL_ASSETS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_14_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_14_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as wealth_14,
                                                                        case
                                                                            when items_new_16_type in (
                                                                                'BUSINESS_PROFITS_DIVIDENDS_OR_SALES',
                                                                                'STOCKS_SHARES_AND_FUNDS',
                                                                                'OTHER',
                                                                                'INHERITANCE_OR_GIFTS',
                                                                                'PREMIUM_ASSETS',
                                                                                'PROPERTY_SALES',
                                                                                'VIRTUAL_ASSETS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_16_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_16_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as wealth_16,
                                                                        case
                                                                            when items_new_9_type in (
                                                                                'BUSINESS_PROFITS_DIVIDENDS_OR_SALES',
                                                                                'STOCKS_SHARES_AND_FUNDS',
                                                                                'OTHER',
                                                                                'INHERITANCE_OR_GIFTS',
                                                                                'PREMIUM_ASSETS',
                                                                                'PROPERTY_SALES',
                                                                                'VIRTUAL_ASSETS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_9_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_9_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as wealth_9,
                                                                        case
                                                                            when items_new_17_type in (
                                                                                'BUSINESS_PROFITS_DIVIDENDS_OR_SALES',
                                                                                'STOCKS_SHARES_AND_FUNDS',
                                                                                'OTHER',
                                                                                'INHERITANCE_OR_GIFTS',
                                                                                'PREMIUM_ASSETS',
                                                                                'PROPERTY_SALES',
                                                                                'VIRTUAL_ASSETS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_17_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_17_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as wealth_17,
                                                                        case
                                                                            when items_new_23_type in (
                                                                                'BUSINESS_PROFITS_DIVIDENDS_OR_SALES',
                                                                                'STOCKS_SHARES_AND_FUNDS',
                                                                                'OTHER',
                                                                                'INHERITANCE_OR_GIFTS',
                                                                                'PREMIUM_ASSETS',
                                                                                'PROPERTY_SALES',
                                                                                'VIRTUAL_ASSETS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_23_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_23_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as wealth_23,
                                                                        case
                                                                            when items_new_26_type in (
                                                                                'BUSINESS_PROFITS_DIVIDENDS_OR_SALES',
                                                                                'STOCKS_SHARES_AND_FUNDS',
                                                                                'OTHER',
                                                                                'INHERITANCE_OR_GIFTS',
                                                                                'PREMIUM_ASSETS',
                                                                                'PROPERTY_SALES',
                                                                                'VIRTUAL_ASSETS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_26_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_26_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as wealth_26,
                                                                        case
                                                                            when items_new_7_type in (
                                                                                'BUSINESS_PROFITS_DIVIDENDS_OR_SALES',
                                                                                'STOCKS_SHARES_AND_FUNDS',
                                                                                'OTHER',
                                                                                'INHERITANCE_OR_GIFTS',
                                                                                'PREMIUM_ASSETS',
                                                                                'PROPERTY_SALES',
                                                                                'VIRTUAL_ASSETS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_7_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_7_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as wealth_7,
                                                                        case
                                                                            when items_new_8_type in (
                                                                                'BUSINESS_PROFITS_DIVIDENDS_OR_SALES',
                                                                                'STOCKS_SHARES_AND_FUNDS',
                                                                                'OTHER',
                                                                                'INHERITANCE_OR_GIFTS',
                                                                                'PREMIUM_ASSETS',
                                                                                'PROPERTY_SALES',
                                                                                'VIRTUAL_ASSETS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_8_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_8_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as wealth_8,
                                                                        case
                                                                            when items_new_21_type in (
                                                                                'BUSINESS_PROFITS_DIVIDENDS_OR_SALES',
                                                                                'STOCKS_SHARES_AND_FUNDS',
                                                                                'OTHER',
                                                                                'INHERITANCE_OR_GIFTS',
                                                                                'PREMIUM_ASSETS',
                                                                                'PROPERTY_SALES',
                                                                                'VIRTUAL_ASSETS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_21_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_21_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as wealth_21,
                                                                        case
                                                                            when items_new_22_type in (
                                                                                'BUSINESS_PROFITS_DIVIDENDS_OR_SALES',
                                                                                'STOCKS_SHARES_AND_FUNDS',
                                                                                'OTHER',
                                                                                'INHERITANCE_OR_GIFTS',
                                                                                'PREMIUM_ASSETS',
                                                                                'PROPERTY_SALES',
                                                                                'VIRTUAL_ASSETS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_22_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_22_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as wealth_22,
                                                                        case
                                                                            when items_new_24_type in (
                                                                                'BUSINESS_PROFITS_DIVIDENDS_OR_SALES',
                                                                                'STOCKS_SHARES_AND_FUNDS',
                                                                                'OTHER',
                                                                                'INHERITANCE_OR_GIFTS',
                                                                                'PREMIUM_ASSETS',
                                                                                'PROPERTY_SALES',
                                                                                'VIRTUAL_ASSETS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_24_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_24_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as wealth_24,
                                                                        case
                                                                            when items_new_3_type in (
                                                                                'BUSINESS_PROFITS_DIVIDENDS_OR_SALES',
                                                                                'STOCKS_SHARES_AND_FUNDS',
                                                                                'OTHER',
                                                                                'INHERITANCE_OR_GIFTS',
                                                                                'PREMIUM_ASSETS',
                                                                                'PROPERTY_SALES',
                                                                                'VIRTUAL_ASSETS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_3_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_3_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as wealth_3,
                                                                        case
                                                                            when items_new_11_type in (
                                                                                'BUSINESS_PROFITS_DIVIDENDS_OR_SALES',
                                                                                'STOCKS_SHARES_AND_FUNDS',
                                                                                'OTHER',
                                                                                'INHERITANCE_OR_GIFTS',
                                                                                'PREMIUM_ASSETS',
                                                                                'PROPERTY_SALES',
                                                                                'VIRTUAL_ASSETS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_11_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_11_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as wealth_11,
                                                                        case
                                                                            when items_new_13_type in (
                                                                                'BUSINESS_PROFITS_DIVIDENDS_OR_SALES',
                                                                                'STOCKS_SHARES_AND_FUNDS',
                                                                                'OTHER',
                                                                                'INHERITANCE_OR_GIFTS',
                                                                                'PREMIUM_ASSETS',
                                                                                'PROPERTY_SALES',
                                                                                'VIRTUAL_ASSETS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_13_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_13_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as wealth_13,
                                                                        case
                                                                            when items_new_19_type in (
                                                                                'BUSINESS_PROFITS_DIVIDENDS_OR_SALES',
                                                                                'STOCKS_SHARES_AND_FUNDS',
                                                                                'OTHER',
                                                                                'INHERITANCE_OR_GIFTS',
                                                                                'PREMIUM_ASSETS',
                                                                                'PROPERTY_SALES',
                                                                                'VIRTUAL_ASSETS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_19_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_19_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as wealth_19,
                                                                        case
                                                                            when items_new_25_type in (
                                                                                'BUSINESS_PROFITS_DIVIDENDS_OR_SALES',
                                                                                'STOCKS_SHARES_AND_FUNDS',
                                                                                'OTHER',
                                                                                'INHERITANCE_OR_GIFTS',
                                                                                'PREMIUM_ASSETS',
                                                                                'PROPERTY_SALES',
                                                                                'VIRTUAL_ASSETS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_25_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_25_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as wealth_25,
                                                                        case
                                                                            when items_new_29_type in (
                                                                                'BUSINESS_PROFITS_DIVIDENDS_OR_SALES',
                                                                                'STOCKS_SHARES_AND_FUNDS',
                                                                                'OTHER',
                                                                                'INHERITANCE_OR_GIFTS',
                                                                                'PREMIUM_ASSETS',
                                                                                'PROPERTY_SALES',
                                                                                'VIRTUAL_ASSETS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_29_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_29_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as wealth_29,
                                                                        case
                                                                            when items_new_6_type in (
                                                                                'BUSINESS_PROFITS_DIVIDENDS_OR_SALES',
                                                                                'STOCKS_SHARES_AND_FUNDS',
                                                                                'OTHER',
                                                                                'INHERITANCE_OR_GIFTS',
                                                                                'PREMIUM_ASSETS',
                                                                                'PROPERTY_SALES',
                                                                                'VIRTUAL_ASSETS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_6_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_6_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as wealth_6,
                                                                        case
                                                                            when items_new_15_type in (
                                                                                'BUSINESS_PROFITS_DIVIDENDS_OR_SALES',
                                                                                'STOCKS_SHARES_AND_FUNDS',
                                                                                'OTHER',
                                                                                'INHERITANCE_OR_GIFTS',
                                                                                'PREMIUM_ASSETS',
                                                                                'PROPERTY_SALES',
                                                                                'VIRTUAL_ASSETS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_15_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_15_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as wealth_15,
                                                                        case
                                                                            when items_new_18_type in (
                                                                                'BUSINESS_PROFITS_DIVIDENDS_OR_SALES',
                                                                                'STOCKS_SHARES_AND_FUNDS',
                                                                                'OTHER',
                                                                                'INHERITANCE_OR_GIFTS',
                                                                                'PREMIUM_ASSETS',
                                                                                'PROPERTY_SALES',
                                                                                'VIRTUAL_ASSETS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_18_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_18_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as wealth_18,
                                                                        case
                                                                            when items_new_28_type in (
                                                                                'BUSINESS_PROFITS_DIVIDENDS_OR_SALES',
                                                                                'STOCKS_SHARES_AND_FUNDS',
                                                                                'OTHER',
                                                                                'INHERITANCE_OR_GIFTS',
                                                                                'PREMIUM_ASSETS',
                                                                                'PROPERTY_SALES',
                                                                                'VIRTUAL_ASSETS'
                                                                            ) then (
                                                                                try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_28_converted_currency, '_'), 2) as decimal(18, 3)
                                                                                ) + try_cast(
                                                                                    ELEMENT_AT(SPLIT(items_new_28_converted_currency, '_'), 3) as decimal(18, 3)
                                                                                )
                                                                            ) / 2
                                                                        end as wealth_28,
                                                                        --- Calculate Average Net worth in GBP:
                                                                        case
                                                                            when estimated_net_worth_currency_code = 'USD' then cast(estimated_net_worth_min_local as decimal (18, 3)) * 0.80
                                                                            when estimated_net_worth_currency_code = 'EUR' then cast(estimated_net_worth_min_local as decimal (18, 3)) * 0.86
                                                                            when estimated_net_worth_currency_code = 'AED' then cast(estimated_net_worth_min_local as decimal (18, 3)) * 0.22
                                                                            when estimated_net_worth_currency_code = 'KWD' then cast(estimated_net_worth_min_local as decimal (18, 3)) * 2.65
                                                                            when estimated_net_worth_currency_code = 'SAR' then cast(estimated_net_worth_min_local as decimal (18, 3)) * 0.22
                                                                            else cast(estimated_net_worth_min_local as decimal (18, 3))
                                                                        end as converted_estimated_net_worth_min_local,
                                                                        case
                                                                            when estimated_net_worth_currency_code = 'USD' then cast(estimated_net_worth_max_local as decimal (18, 3)) * 0.80
                                                                            when estimated_net_worth_currency_code = 'EUR' then cast(estimated_net_worth_max_local as decimal (18, 3)) * 0.86
                                                                            when estimated_net_worth_currency_code = 'AED' then cast(estimated_net_worth_max_local as decimal (18, 3)) * 0.22
                                                                            when estimated_net_worth_currency_code = 'KWD' then cast(estimated_net_worth_max_local as decimal (18, 3)) * 2.65
                                                                            when estimated_net_worth_currency_code = 'SAR' then cast(estimated_net_worth_max_local as decimal (18, 3)) * 0.22
                                                                            else cast(estimated_net_worth_max_local as decimal (18, 3))
                                                                        end as converted_estimated_net_worth_max_local
                                                                    from
                                                                        datalake_curated.dynamo_sls_riskscore
                                                                    where
                                                                        dynamodb_keys_customer_id_s is not null
                                                                )
                                                        ),
                                                        get_pivot_ as (
                                                            select
                                                                t.dynamodb_keys_customer_id_s,
                                                                r.type as income_wealth_type,
                                                                net_worth,
                                                                r.value as income_wealth_average
                                                            from
                                                                get_distinct t
                                                                cross join unnest(
                                                                    array [ cast(row('income', t."income_0") as row(type varchar, value varchar)),
cast(row('income', t."income_1") as row(type varchar, value varchar)),
cast(row('income', t."income_5") as row(type varchar, value varchar)),
cast(row('income', t."income_27") as row(type varchar, value varchar)),
cast(row('income', t."income_2") as row(type varchar, value varchar)),
cast(row('income', t."income_4") as row(type varchar, value varchar)),
cast(row('income', t."income_20") as row(type varchar, value varchar)),
cast(row('income', t."income_10") as row(type varchar, value varchar)),
cast(row('income', t."income_12") as row(type varchar, value varchar)),
cast(row('income', t."income_14") as row(type varchar, value varchar)),
cast(row('income', t."income_16") as row(type varchar, value varchar)),
cast(row('income', t."income_9") as row(type varchar, value varchar)),
cast(row('income', t."income_17") as row(type varchar, value varchar)),
cast(row('income', t."income_23") as row(type varchar, value varchar)),
cast(row('income', t."income_26") as row(type varchar, value varchar)),
cast(row('income', t."income_7") as row(type varchar, value varchar)),
cast(row('income', t."income_8") as row(type varchar, value varchar)),
cast(row('income', t."income_21") as row(type varchar, value varchar)),
cast(row('income', t."income_22") as row(type varchar, value varchar)),
cast(row('income', t."income_24") as row(type varchar, value varchar)),
cast(row('income', t."income_3") as row(type varchar, value varchar)),
cast(row('income', t."income_11") as row(type varchar, value varchar)),
cast(row('income', t."income_13") as row(type varchar, value varchar)),
cast(row('income', t."income_19") as row(type varchar, value varchar)),
cast(row('income', t."income_25") as row(type varchar, value varchar)),
cast(row('income', t."income_29") as row(type varchar, value varchar)),
cast(row('income', t."income_6") as row(type varchar, value varchar)),
cast(row('income', t."income_15") as row(type varchar, value varchar)),
cast(row('income', t."income_18") as row(type varchar, value varchar)),
cast(row('income', t."income_28") as row(type varchar, value varchar)),
cast(row('wealth', t."wealth_0") as row(type varchar, value varchar)),
cast(row('wealth', t."wealth_1") as row(type varchar, value varchar)),
cast(row('wealth', t."wealth_5") as row(type varchar, value varchar)),
cast(row('wealth', t."wealth_27") as row(type varchar, value varchar)),
cast(row('wealth', t."wealth_2") as row(type varchar, value varchar)),
cast(row('wealth', t."wealth_4") as row(type varchar, value varchar)),
cast(row('wealth', t."wealth_20") as row(type varchar, value varchar)),
cast(row('wealth', t."wealth_10") as row(type varchar, value varchar)),
cast(row('wealth', t."wealth_12") as row(type varchar, value varchar)),
cast(row('wealth', t."wealth_14") as row(type varchar, value varchar)),
cast(row('wealth', t."wealth_16") as row(type varchar, value varchar)),
cast(row('wealth', t."wealth_9") as row(type varchar, value varchar)),
cast(row('wealth', t."wealth_17") as row(type varchar, value varchar)),
cast(row('wealth', t."wealth_23") as row(type varchar, value varchar)),
cast(row('wealth', t."wealth_26") as row(type varchar, value varchar)),
cast(row('wealth', t."wealth_7") as row(type varchar, value varchar)),
cast(row('wealth', t."wealth_8") as row(type varchar, value varchar)),
cast(row('wealth', t."wealth_21") as row(type varchar, value varchar)),
cast(row('wealth', t."wealth_22") as row(type varchar, value varchar)),
cast(row('wealth', t."wealth_24") as row(type varchar, value varchar)),
cast(row('wealth', t."wealth_3") as row(type varchar, value varchar)),
cast(row('wealth', t."wealth_11") as row(type varchar, value varchar)),
cast(row('wealth', t."wealth_13") as row(type varchar, value varchar)),
cast(row('wealth', t."wealth_19") as row(type varchar, value varchar)),
cast(row('wealth', t."wealth_25") as row(type varchar, value varchar)),
cast(row('wealth', t."wealth_29") as row(type varchar, value varchar)),
cast(row('wealth', t."wealth_6") as row(type varchar, value varchar)),
cast(row('wealth', t."wealth_15") as row(type varchar, value varchar)),
cast(row('wealth', t."wealth_18") as row(type varchar, value varchar)),
cast(row('wealth', t."wealth_28") as row(type varchar, value varchar)),
cast(row('average_net_worth', t."net_worth") as row(type varchar, value varchar))
                  ]
                                                                ) u(r)
                                                        )
                                                        select
                                                            dynamodb_keys_customer_id_s,
                                                            income_wealth_type,
                                                            sum(cast(income_wealth_average as decimal(18, 3))) as income_wealth_sum_of_average
                                                        from
                                                            get_pivot_
                                                        where
                                                            income_wealth_type is not null
                                                            and income_wealth_average is not null
                                                        group by
                                                            dynamodb_keys_customer_id_s,
                                                            income_wealth_type
                                                    ) AS Income_wealth
                                                group by
                                                    dynamodb_keys_customer_id_s
                                            )
                                    ),
                                    ---- This is added due to an exception required as customers Income and Assets are empty for latest record but populated in the onboarding journey --------
                                    --   therefore rn_last will miss these values populated ----------
                                    Max_Income_Assets_Exception as (
                                        select
                                            user_id,
                                            max(Income_GBP_Cleaned) as Max_Income_GBP_Cleaned,
                                            max(Assets_GBP_Cleaned) as Max_Assets_GBP_Cleaned
                                        from
                                            customer_table_income
                                        group by
                                            1
                                    ),
                                    -------------------------------------
                                    last_rn as (
                                        select
                                            a.*,
                                            b.Max_Income_GBP_Cleaned,
                                            b.Max_Assets_GBP_Cleaned
                                        from
                                            customer_table_income as a
                                            left join Max_Income_Assets_Exception as b on a.user_id = b.user_id
                                        where
                                            rn_last = 1
                                    ),
                                    Combined_Income as (
                                        select
                                            a.user_id,
                                            dynamodb_new_image_individual_m_address_m_country_code_s,
                                            dynamodb_new_image_status_s,
                                            dynamodb_new_image_card_ordered_bool,
                                            average_net_worth,
                                            case
                                                when (
                                                    dynamodb_new_image_individual_m_address_m_country_code_s = 'ARE'
                                                    and income is not null
                                                    and income <> 0
                                                ) then income
                                                when Income_GBP_Cleaned > 0 then Income_GBP_Cleaned
                                                when Max_Income_GBP_Cleaned > 0 then Max_Income_GBP_Cleaned
                                                else income
                                            end as Income_GBP,
                                            case
                                                when (
                                                    dynamodb_new_image_individual_m_address_m_country_code_s = 'ARE'
                                                    and wealth > 0
                                                ) then wealth
                                                when (
                                                    wealth = 0
                                                    or wealth is null
                                                )
                                                and average_net_worth > 0 then average_net_worth
                                                when Assets_GBP_Cleaned > 0 then Assets_GBP_Cleaned
                                                when Max_Assets_GBP_Cleaned > 0 then Max_Assets_GBP_Cleaned
                                                else wealth
                                            end as Assets_GBP,
                                            case
                                                when dynamodb_new_image_individual_m_address_m_country_code_s = 'ARE'
                                                and (
                                                    (
                                                        income is null
                                                        or income = 0
                                                    )
                                                    and (
                                                        wealth is null
                                                        or wealth = 0
                                                    )
                                                ) then 1
                                                else 0
                                            end as UAE_Cust_No_Income_Wealth_Riskscore,
                                            case
                                                when dynamodb_new_image_individual_m_address_m_country_code_s = 'ARE'
                                                and (
                                                    (income > 0)
                                                    and (
                                                        wealth is null
                                                        or wealth = 0
                                                    )
                                                ) then 1
                                                else 0
                                            end as UAE_Cust_Zero_Wealth
                                        from
                                            last_rn a
                                            left join income_wealth_wide b on a.user_id = b.dynamodb_keys_customer_id_s
                                    ),
                                    Combined_Income_nulls_cleaned as (
                                        select
                                            user_id,
                                            dynamodb_new_image_individual_m_address_m_country_code_s as country_code,
                                            dynamodb_new_image_status_s as current_status,
                                            dynamodb_new_image_card_ordered_bool as card_ordered_ind,
                                            COALESCE(UAE_Cust_No_Income_Wealth_Riskscore, 0) as UAE_Cust_No_Income_Wealth_Riskscore,
                                            COALESCE(UAE_Cust_Zero_Wealth, 0) as UAE_Cust_Zero_Wealth,
                                            CASE
                                                WHEN Income_GBP is NULL
                                                and Assets_GBP IS NOT NULL THEN 0
                                                else Income_GBP
                                            END AS Income_GBP_Value,
                                            CASE
                                                WHEN Assets_GBP is NULL
                                                and Income_GBP IS NOT NULL THEN 0
                                                else Assets_GBP
                                            END AS Assets_GBP_Value,
                                            average_net_worth
                                        from
                                            Combined_Income
                                    )
                                    select
                                        *,
                                        CASE
                                            when Income_GBP_Value IS NULL
                                            and Assets_GBP_Value IS NULL then NULL
                                            WHEN Income_GBP_Value >= 100000
                                            or Assets_GBP_Value >= 200000 then 'Affluent'
                                            WHEN Income_GBP_Value >= 50000
                                            or Assets_GBP_Value >= 60000 then 'Mass Affluent'
                                            WHEN Income_GBP_Value < 50000
                                            or Assets_GBP_Value < 60000 then 'Mass'
                                            ELSE NULL
                                        END as Affluency
                                    from
                                        Combined_Income_nulls_cleaned
                                ) AS Income_Assests_All_Jurisdictions
                        ) as rg ON a.dynamo_user_key = rg.user_id
                        Left Join (
                            select
                                *,
                                count(income_wealth_value) over(
                                    partition by dynamodb_keys_customer_id_s,
                                    income_wealth_type
                                ) as income_wealth_source_count
                            from
                                (
                                    select
                                        distinct *,
                                        case
                                            when income_wealth_value in (
                                                'INCOME_FROM_SALARY_BONUS_BENEFITS_COMMISSIONS',
                                                'INCOME_FROM_PERSONAL_SAVINGS',
                                                'SAVINGS_FROM_SALARY_OR_EARNINGS'
                                            ) then 'income'
                                            else 'wealth'
                                        end as income_wealth_type
                                    from
                                        (
                                            with get_distinct as (
                                                select
                                                    distinct dynamodb_keys_customer_id_s,
                                                    items_new_0_type,
                                                    items_new_1_type,
                                                    items_new_5_type,
                                                    items_new_27_type,
                                                    items_new_2_type,
                                                    items_new_4_type,
                                                    items_new_20_type,
                                                    items_new_10_type,
                                                    items_new_12_type,
                                                    items_new_14_type,
                                                    items_new_16_type,
                                                    items_new_9_type,
                                                    items_new_17_type,
                                                    items_new_23_type,
                                                    items_new_26_type,
                                                    items_new_7_type,
                                                    items_new_8_type,
                                                    items_new_21_type,
                                                    items_new_22_type,
                                                    items_new_24_type,
                                                    items_new_3_type,
                                                    items_new_11_type,
                                                    items_new_13_type,
                                                    items_new_19_type,
                                                    items_new_25_type,
                                                    items_new_29_type,
                                                    items_new_6_type,
                                                    items_new_15_type,
                                                    items_new_18_type,
                                                    items_new_28_type
                                                from
                                                    datalake_curated.dynamo_sls_riskscore
                                                where
                                                    dynamodb_keys_customer_id_s is not null
                                            ),
                                            get_pivot_ as (
                                                select
                                                    t.dynamodb_keys_customer_id_s,
                                                    r.type as income_wealth_type,
                                                    r.value as income_wealth_value
                                                from
                                                    get_distinct t
                                                    cross join unnest(
                                                        array [
cast(row('items_new_0_type', t."items_new_0_type") as row(type varchar, value varchar)),
cast(row('items_new_1_type', t."items_new_1_type") as row(type varchar, value varchar)),
cast(row('items_new_5_type', t."items_new_5_type") as row(type varchar, value varchar)),
cast(row('items_new_27_type', t."items_new_27_type") as row(type varchar, value varchar)),
cast(row('items_new_2_type', t."items_new_2_type") as row(type varchar, value varchar)),
cast(row('items_new_4_type', t."items_new_4_type") as row(type varchar, value varchar)),
cast(row('items_new_20_type', t."items_new_20_type") as row(type varchar, value varchar)),
cast(row('items_new_10_type', t."items_new_10_type") as row(type varchar, value varchar)),
cast(row('items_new_12_type', t."items_new_12_type") as row(type varchar, value varchar)),
cast(row('items_new_14_type', t."items_new_14_type") as row(type varchar, value varchar)),
cast(row('items_new_16_type', t."items_new_16_type") as row(type varchar, value varchar)),
cast(row('items_new_9_type', t."items_new_9_type") as row(type varchar, value varchar)),
cast(row('items_new_17_type', t."items_new_17_type") as row(type varchar, value varchar)),
cast(row('items_new_23_type', t."items_new_23_type") as row(type varchar, value varchar)),
cast(row('items_new_26_type', t."items_new_26_type") as row(type varchar, value varchar)),
cast(row('items_new_7_type', t."items_new_7_type") as row(type varchar, value varchar)),
cast(row('items_new_8_type', t."items_new_8_type") as row(type varchar, value varchar)),
cast(row('items_new_21_type', t."items_new_21_type") as row(type varchar, value varchar)),
cast(row('items_new_22_type', t."items_new_22_type") as row(type varchar, value varchar)),
cast(row('items_new_24_type', t."items_new_24_type") as row(type varchar, value varchar)),
cast(row('items_new_3_type', t."items_new_3_type") as row(type varchar, value varchar)),
cast(row('items_new_11_type', t."items_new_11_type") as row(type varchar, value varchar)),
cast(row('items_new_13_type', t."items_new_13_type") as row(type varchar, value varchar)),
cast(row('items_new_19_type', t."items_new_19_type") as row(type varchar, value varchar)),
cast(row('items_new_25_type', t."items_new_25_type") as row(type varchar, value varchar)),
cast(row('items_new_29_type', t."items_new_29_type") as row(type varchar, value varchar)),
cast(row('items_new_6_type', t."items_new_6_type") as row(type varchar, value varchar)),
cast(row('items_new_15_type', t."items_new_15_type") as row(type varchar, value varchar)),
cast(row('items_new_18_type', t."items_new_18_type") as row(type varchar, value varchar)),
cast(row('items_new_28_type', t."items_new_28_type") as row(type varchar, value varchar))
 ]
                                                    ) u(r)
                                            )
                                            select
                                                distinct dynamodb_keys_customer_id_s,
                                                income_wealth_value
                                            from
                                                get_pivot_
                                            where
                                                income_wealth_type is not null
                                                and income_wealth_value is not null
                                        ) AS Source_of_wealth_income
                                )
                        ) sr on rg.user_id = sr.dynamodb_keys_customer_id_s
                        left join (
                            select
                                *
                            from
                                risk_form
                        ) b on a.dynamo_user_key = b.dynamodb_new_image_customer_id_s
                        left join occupation_tb occ on a.dynamo_user_key = occ.dynamodb_new_image_customer_id_s
                        left join circumstance_tb cir on a.dynamo_user_key = cir.dynamodb_new_image_customer_id_s
                    order by
                        dynamo_user_key
                )
        ) b on a.Customer_ID = b.user_id
        left join all_cust c on a.Customer_ID = c.user_id
        left join latest_account_state d on a.FTD_Account_ID = d.account_id
        left join per_customer_level e on a.Customer_ID = e.Customer_ID -- where ((date_diff('day',FTD_Maturity_Date,current_date) between -21 and 21) or (date_diff('day',current_date , FTD_Maturity_Date) between -21 and 21))
),
offer_days_rule as (
    select
        dynamodb_new_image_available_days_after_maturity_n,
        ROW_NUMBER() over(
            PARTITION by dynamodb_new_image_product_encoded_key_s,
            dynamodb_new_image_sk_s
            order by
                from_unixtime(cast(dynamodb_new_image_updated_at_n as BIGint)) desc
        ) as rn_last
    from
        datalake_raw.dynamo_default_payment_products
    where
        1 = 1
        and dynamodb_new_image_sk_s = 'ftd:rule:post_maturity_reinvestment'
        and NULLIF(trim(dynamodb_new_image_updated_at_n), '') is not null
),
joined_rates as(
    select
        *,
        (
            select
                dynamodb_new_image_available_days_after_maturity_n
            from
                offer_days_rule
            where
                rn_last = 1
        ) as FTD_Offer_Days
    from
        final_tb_1
        cross join ftd_rate_pivot_rows_to_columns
        cross join unioned_agg_pivot
)
select
    *,
    case
        when FTD_Offer_Days <> -1 then DATE_ADD(
            'day',
            FTD_Offer_Days,
            FTD_Maturity_Date_plus_1hr
        )
    end AS FTD_Offer_Expired_Date
from
    joined_rates
where
    1 = 1
