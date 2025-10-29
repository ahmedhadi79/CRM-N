with Taxonomy_Base as (
    SELECT
        DISTINCT User_ID,
        Age_Range,
        Gender,
        Card_Ordered,
        Nationality,
        Country_Code,
        Brand_ID,
        User_Agent,
        Current_Status,
        Income_GBP_Value,
        Assets_GBP_Value,
        Affluency,
        C_c,
        CASE
            WHEN Current_Status = 'APPROVED' THEN 1
            ELSE 0
        END as Approved_Customer
    FROM
        (
            select
                rg.*,
                case
                    when dynamodb_individual_m_nationality_country_code_s = '' then REPLACE(
                        dynamodb_individual_m_nationality_country_code_s,
                        '',
                        'Unknown'
                    )
                    else dynamodb_individual_m_nationality_country_code_s
                end as Nationality,
                case
                    when age_range = '' then REPLACE(age_range, '', 'Unknown')
                    else age_range
                end as Age_Range,
                COALESCE(
                    nullif(nullif(ltrim(rtrim(brand_id)), ''), '<NA>'),
                    'NOMO_BANK'
                ) as brand_id,
                case
                    when dynamodb_new_gid = '1' then 'Female'
                    when dynamodb_new_gid = '0' then 'Male'
                end as gender,
                case
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
                END AS income_wealth_type
            from
                datalake_curated.customer_timeline_detail a
                right join (
                    select
                        *
                    from
                        (
                            with customer_table_income as (
                                select
                                    COALESCE(dynamodb_keys_id_s, dynamodb_key_id_s) as user_id,
                                    case
                                        when dynamodb_new_image_tos_acceptance_m_user_agent_s like '%Mozilla%'
                                        and dynamodb_new_image_tos_acceptance_m_user_agent_s like '%OS%' then 'IOS'
                                        when dynamodb_new_image_tos_acceptance_m_user_agent_s like '%CFNetwork%'
                                        and dynamodb_new_image_tos_acceptance_m_user_agent_s like '%Darwin%' then 'IOS'
                                        when dynamodb_new_image_tos_acceptance_m_user_agent_s like '%okhttp%' then 'Android'
                                        else dynamodb_new_image_tos_acceptance_m_user_agent_s
                                    end as User_Agent,
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
                                        when dynamodb_new_image_individual_m_financial_details_m_monthly_income_s = '<NA>' then null
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
                                        when dynamodb_new_image_individual_m_financial_details_m_estimated_assets_value_s = '<NA>' then null
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
                                    case
                                        when dynamodb_new_image_card_ordered_bool = true then (5.70 + 15)
                                        else 0
                                    end as C_c -- cost of a card
,
                                    case
                                        when dynamodb_new_image_status_s = 'AWAITING_MANUAL_REVIEW'
                                        and dynamodb_new_image_risk_m_rating_s = 'HIGH' then 'Application_Submitted'
                                        when dynamodb_new_image_status_s = 'AWAITING_APPROVAL'
                                        and dynamodb_new_image_risk_m_rating_s != 'HIGH' then 'Application_Submitted'
                                        else 'Not_Application_Submission_Step'
                                    end as Application_Submitted,
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
                                from
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
                                                                end as wealth_28 --- Calculate Average Net worth in GBP:
,
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
                                                                1 = 1
                                                                and dynamodb_keys_customer_id_s is not null
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
                                                        cross JOIN unnest(
                                                            array [
                                                  cast(row('income', t."income_0") as row(type varchar, value varchar)),
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
                                                    1 = 1
                                                    and income_wealth_type is not null
                                                    and income_wealth_average is not null
                                                group by
                                                    dynamodb_keys_customer_id_s,
                                                    income_wealth_type
                                            ) AS Income_wealth
                                        group by
                                            dynamodb_keys_customer_id_s
                                    )
                            ),
                            last_rn as (
                                select
                                    *
                                from
                                    customer_table_income
                                where
                                    rn_last = 1
                            ),
                            Combined_Income as (
                                select
                                    a.user_id,
                                    User_Agent,
                                    dynamodb_new_image_individual_m_address_m_country_code_s,
                                    dynamodb_new_image_status_s,
                                    dynamodb_new_image_card_ordered_bool,
                                    C_c,
                                    average_net_worth,
                                    case
                                        when (
                                            dynamodb_new_image_individual_m_address_m_country_code_s = 'ARE'
                                            and income is not null
                                            and income <> 0
                                        ) then income
                                        else Income_GBP_Cleaned
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
                                        else Assets_GBP_Cleaned
                                    end as Assets_GBP,
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
                                    User_Agent,
                                    case
                                        when dynamodb_new_image_individual_m_address_m_country_code_s = '' then REPLACE(
                                            dynamodb_new_image_individual_m_address_m_country_code_s,
                                            '',
                                            'Unknown'
                                        )
                                        else COALESCE(
                                            dynamodb_new_image_individual_m_address_m_country_code_s,
                                            'Unknown'
                                        )
                                    end as Country_Code,
                                    dynamodb_new_image_status_s as current_status,
                                    average_net_worth,
                                    COALESCE(UAE_Cust_Zero_Wealth, 0) as UAE_Cust_Zero_Wealth,
                                    CASE
                                        WHEN dynamodb_new_image_card_ordered_bool = TRUE THEN 1
                                        ELSE 0
                                    END as Card_Ordered,
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
                                    C_c
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
                                        1 = 1
                                        and income_wealth_type is not null
                                        and income_wealth_value is not null
                                ) AS Source_of_wealth_income
                        )
                ) sr on rg.user_id = sr.dynamodb_keys_customer_id_s
            order by
                dynamo_user_key
        )
)
select
    distinct user_id,
    coalesce(affluency, 'Unknown') as affluency,
    age_range
from
    taxonomy_base
