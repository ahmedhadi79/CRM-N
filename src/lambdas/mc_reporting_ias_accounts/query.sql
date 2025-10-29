with cust_affluency as (
    select
        distinct user_id,
        COALESCE(Affluency, '') as affluency,
        age_range
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
),
dt as (
    select
        parent_account_key,
        FIRST_VALUE(transaction_date) OVER (
            PARTITION BY parent_account_key
            ORDER BY
                transaction_date asc
        ) as first_deposit_date,
        FIRST_VALUE(transaction_date) OVER (
            PARTITION BY parent_account_key
            ORDER BY
                transaction_date desc
        ) as last_deposit_date
    from
        (
            select
                parent_account_key,
                cast(from_iso8601_timestamp(value_date) as date) as transaction_date,
                ROW_NUMBER() OVER (
                    PARTITION BY dt.id
                    ORDER BY
                        "timestamp_extracted" DESC
                ) AS rn
            from
                datalake_raw.deposit_transactions dt
            where
                1 = 1
                and creation_date is not NULL
                and date(from_iso8601_timestamp(dt.value_date)) >= date('2021-07-01')
                and type = 'DEPOSIT'
        ) a
    where
        1 = 1
        and rn = 1
),
dt_2 as (
    select
        *
    from
        (
            select
                parent_account_key,
                from_iso8601_timestamp(dt.value_date) as transaction_date,
                cast(amount as decimal(18, 2)) as interest_amount,
                ROW_NUMBER() OVER (
                    PARTITION BY dt.id
                    ORDER BY
                        "timestamp_extracted" DESC
                ) AS rn
            from
                datalake_raw.deposit_transactions as dt
            where
                1 = 1
                and creation_date is not NULL
                and date(from_iso8601_timestamp(dt.value_date)) >= date('2021-07-01')
                and type = 'INTEREST_APPLIED'
            order by
                parent_account_key
        ) a
    where
        1 = 1
        and rn = 1
),
interest_aggregated as (
    select
        parent_account_key,
        transaction_date,
        SUM(interest_amount) OVER (
            PARTITION BY parent_account_key
            ORDER BY
                transaction_date ROWS BETWEEN UNBOUNDED PRECEDING
                AND CURRENT ROW
        ) AS Accrued_Interest_Amount,
        row_number() OVER (
            PARTITION BY parent_account_key
            ORDER BY
                transaction_date desc
        ) as rn
    from
        dt_2
    where
        1 = 1
        and transaction_date < date_trunc('month', CURRENT_DATE) + INTERVAL '1' month
    order by
        parent_account_key,
        transaction_date
),
latest_interest_accrued as (
    select
        *
    from
        interest_aggregated
    where
        1 = 1
        and rn = 1
),
ias_tiers as (
    select
        a.name -- name
,
        cast(
            interest_settings_interest_rate_settings_interest_rate_tiers_1_interest_rate as decimal(18, 2)
        ) as ias_tier_1_rate,
        cast(
            interest_settings_interest_rate_settings_interest_rate_tiers_2_interest_rate as decimal(18, 2)
        ) as ias_tier_2_rate,
        cast(
            interest_settings_interest_rate_settings_interest_rate_tiers_3_interest_rate as decimal(18, 2)
        ) as ias_tier_3_rate -- , MAX(cast(from_iso8601_timestamp(last_modified_date) as date)) as latest_interest_rate_date
,
        row_number() OVER (
            PARTITION BY a.name
            ORDER BY
                MAX(
                    cast(
                        from_iso8601_timestamp(last_modified_date) as date
                    )
                ) desc
        ) as rn
    from
        datalake_raw.deposit_accounts as a
        inner join (
            select
                distinct id as user_id,
                encoded_key
            from
                datalake_raw.clients
        ) as c on a.account_holder_key = c.encoded_key
    where
        1 = 1
        and name like '%Instant%'
    group by
        a.name,
        interest_settings_interest_rate_settings_interest_rate_tiers_1_interest_rate,
        interest_settings_interest_rate_settings_interest_rate_tiers_2_interest_rate,
        interest_settings_interest_rate_settings_interest_rate_tiers_3_interest_rate
),
latest_ias_tiers as (
    select
        *,
        CASE
            WHEN ias_tier_1_rate >= ias_tier_2_rate
            AND ias_tier_1_rate >= ias_tier_3_rate THEN ias_tier_1_rate
            WHEN ias_tier_2_rate >= ias_tier_1_rate
            AND ias_tier_2_rate >= ias_tier_3_rate THEN ias_tier_2_rate
            ELSE ias_tier_3_rate
        END AS top_ias_interest_rate
    from
        ias_tiers
    where
        1 = 1
        and rn = 1
),
ias_rates as (
    select
        name ---- GBP ----
,
        max(
            case
                when name like 'GBP%' then concat(cast(ias_tier_1_rate as varchar), '%')
            end
        ) as IAS_Dynamic_Rate_GBP_Tier_1,
        max(
            case
                when name like 'GBP%' then concat(cast(ias_tier_2_rate as varchar), '%')
            end
        ) as IAS_Dynamic_Rate_GBP_Tier_2,
        max(
            case
                when name like 'GBP%' then concat(cast(ias_tier_3_rate as varchar), '%')
            end
        ) as IAS_Dynamic_Rate_GBP_Tier_3,
        max(
            case
                when name like 'GBP%' then concat(cast(top_ias_interest_rate as varchar), '%')
            end
        ) as IAS_Dynamic_Rate_GBP_Top_Rate ---- USD ----
,
        max(
            case
                when name like 'USD%' then concat(cast(ias_tier_1_rate as varchar), '%')
            end
        ) as IAS_Dynamic_Rate_USD_Tier_1,
        max(
            case
                when name like 'USD%' then concat(cast(ias_tier_2_rate as varchar), '%')
            end
        ) as IAS_Dynamic_Rate_USD_Tier_2,
        max(
            case
                when name like 'USD%' then concat(cast(ias_tier_3_rate as varchar), '%')
            end
        ) as IAS_Dynamic_Rate_USD_Tier_3,
        max(
            case
                when name like 'USD%' then concat(cast(top_ias_interest_rate as varchar), '%')
            end
        ) as IAS_Dynamic_Rate_USD_Top_Rate ---- EUR ----
,
        max(
            case
                when name like 'EUR%' then concat(cast(ias_tier_1_rate as varchar), '%')
            end
        ) as IAS_Dynamic_Rate_EUR_Tier_1,
        max(
            case
                when name like 'EUR%' then concat(cast(ias_tier_2_rate as varchar), '%')
            end
        ) as IAS_Dynamic_Rate_EUR_Tier_2,
        max(
            case
                when name like 'EUR%' then concat(cast(ias_tier_3_rate as varchar), '%')
            end
        ) as IAS_Dynamic_Rate_EUR_Tier_3,
        max(
            case
                when name like 'EUR%' then concat(cast(top_ias_interest_rate as varchar), '%')
            end
        ) as IAS_Dynamic_Rate_EUR_Top_Rate
    from
        latest_ias_tiers
    group by
        name
),
da as (
    select
        distinct c.user_id as IAS_Customer_ID,
        a.encoded_key as IAS_Encoded_Key,
        a.id as IAS_Account_ID,
        a.name as IAS_Account_Name,
        a.currency_code as IAS_Currency_Code,
        cast(
            FIRST_VALUE(from_iso8601_timestamp(a.activation_date)) OVER (
                partition by a.account_holder_key,
                a.encoded_key
                order by
                    from_iso8601_timestamp(last_modified_date) ASC
            ) as date
        ) as IAS_Open_Date,
        FIRST_VALUE(account_state) OVER (
            partition by a.encoded_key
            order by
                from_iso8601_timestamp(last_modified_date) DESC
        ) as IAS_Account_State ---- IAS GBP ----
,
        case
            when a.name like 'GBP%'
            AND FIRST_VALUE(account_state) OVER (
                partition by a.account_holder_key,
                a.encoded_key
                order by
                    from_iso8601_timestamp(last_modified_date) DESC
            ) = 'ACTIVE' THEN 1
            else 0
        end as IAS_GBP_is_Open,
        case
            when a.name like 'GBP%' THEN dt.first_deposit_date
            else null
        end as IAS_GBP_First_Deposit_Date,
        case
            when a.name like 'GBP%' THEN dt.last_deposit_date
            else null
        end as IAS_GBP_Last_Transaction_Date,
        case
            when a.name like 'GBP%' THEN FIRST_VALUE(balances_total_balance) OVER (
                partition by a.account_holder_key,
                a.encoded_key
                order by
                    from_iso8601_timestamp(last_modified_date) DESC
            )
            else null
        end as IAS_GBP_Current_Balance ---- IAS USD ----
,
        case
            when a.name like 'USD%'
            AND FIRST_VALUE(account_state) OVER (
                partition by a.account_holder_key,
                a.encoded_key
                order by
                    from_iso8601_timestamp(last_modified_date) DESC
            ) = 'ACTIVE' THEN 1
            else 0
        end as IAS_USD_is_Open,
        case
            when a.name like 'USD%' THEN dt.first_deposit_date
            else null
        end as IAS_USD_First_Deposit_Date,
        case
            when a.name like 'USD%' THEN dt.last_deposit_date
            else null
        end as IAS_USD_Last_Transaction_Date,
        case
            when a.name like 'USD%' THEN FIRST_VALUE(balances_total_balance) OVER (
                partition by a.account_holder_key,
                a.encoded_key
                order by
                    from_iso8601_timestamp(last_modified_date) DESC
            )
            else null
        end as IAS_USD_Current_Balance ---- IAS USD ----
,
        case
            when a.name like 'EUR%'
            AND FIRST_VALUE(account_state) OVER (
                partition by a.account_holder_key,
                a.encoded_key
                order by
                    from_iso8601_timestamp(last_modified_date) DESC
            ) = 'ACTIVE' THEN 1
            else 0
        end as IAS_EUR_is_Open,
        case
            when a.name like 'EUR%' THEN dt.first_deposit_date
            else null
        end as IAS_EUR_First_Deposit_Date,
        case
            when a.name like 'EUR%' THEN dt.last_deposit_date
            else null
        end as IAS_EUR_Last_Deposit_Date,
        case
            when a.name like 'EUR%' THEN FIRST_VALUE(balances_total_balance) OVER (
                partition by a.account_holder_key,
                a.encoded_key
                order by
                    from_iso8601_timestamp(last_modified_date) DESC
            )
            else null
        end as IAS_EUR_Current_Balance -- , row_number() over(partition by account_holder_key, a.encoded_key order by from_iso8601_timestamp(last_modified_date) asc) as rn_first
        -- , row_number() over(partition by account_holder_key, a.encoded_key order by from_iso8601_timestamp(last_modified_date) desc) as rn_last
        -- , row_number() over(partition by account_holder_key, a.encoded_key, a.account_state order by from_iso8601_timestamp(last_modified_date) asc) as rn_first_state
    from
        datalake_raw.deposit_accounts as a
        inner join (
            select
                distinct id as user_id,
                encoded_key
            from
                datalake_raw.clients
        ) as c on a.account_holder_key = c.encoded_key
        inner join dt as dt on dt.parent_account_key = a.encoded_key
    where
        1 = 1
        and name like '%Instant%' --and user_id = '4YMLAFiR1D6oy7BVy4xGTs'
        -- order by last_modified_date
),
IAS_aggregated as (
    select
        da.*,
        SUM(
            case
                when IAS_Account_State = 'ACTIVE' THEN 1
                else 0
            end
        ) OVER (PARTITION BY IAS_Customer_ID) as IAS_No_Active_Accounts -- , COUNT(IAS_Encoded_Key) OVER (PARTITION BY IAS_Customer_ID) as IAS_All_Accounts
,
        case
            when SUM(
                case
                    when IAS_Account_State = 'ACTIVE' THEN 1
                    else 0
                end
            ) OVER (PARTITION BY IAS_Customer_ID) < COUNT(IAS_Encoded_Key) OVER (PARTITION BY IAS_Customer_ID) THEN 1
            else 0
        end as IAS_Has_Opened_Prev
    from
        da as da
),
ias_recurring as (
    select
        *
    from
        (
            select
                dynamodb_new_image_user_id_s,
                cast(
                    dynamodb_new_image_target_account_id_s as varchar
                ) as account_id,
                dynamodb_new_image_is_active_bool as ias_recurring_flag,
                row_number() OVER (
                    PARTITION BY cast(
                        dynamodb_new_image_target_account_id_s as varchar
                    )
                    order by
                        from_unixtime(
                            cast(dynamodb_new_image_updated_at_n as bigint) / 1000
                        ) desc
                ) as rn
            from
                datalake_raw.dynamo_sls_ias_recurring
        )
    where
        1 = 1
        and rn = 1
)
select
    IAS_Customer_ID,
    IAS_Encoded_Key,
    IAS_Account_ID,
    IAS_Account_Name,
    IAS_Currency_Code,
    CASE
        WHEN ias_currency_code = 'GBP'
        and IAS_GBP_is_open = 1
        and CAST(IAS_GBP_Current_Balance as double) >= 3000
        and CAST(IAS_GBP_Current_Balance as double) < 10000 then IAS_dynamic_rate_gbp_tier_1
        WHEN ias_currency_code = 'GBP'
        and IAS_GBP_is_open = 1
        and CAST(IAS_GBP_Current_Balance as double) >= 10000
        and CAST(IAS_GBP_Current_Balance as double) < 25000 then IAS_dynamic_rate_gbp_tier_2
        WHEN ias_currency_code = 'GBP'
        and IAS_GBP_is_open = 1
        and CAST(IAS_GBP_Current_Balance as double) >= 25000 then IAS_dynamic_rate_gbp_tier_3
        WHEN ias_currency_code = 'USD'
        and IAS_USD_is_open = 1
        and CAST(IAS_USD_Current_Balance as double) >= 3000
        and CAST(IAS_USD_Current_Balance as double) < 10000 then IAS_dynamic_rate_usd_tier_1
        WHEN ias_currency_code = 'USD'
        and IAS_USD_is_open = 1
        and CAST(IAS_USD_Current_Balance as double) >= 10000
        and CAST(IAS_USD_Current_Balance as double) < 30000 then IAS_dynamic_rate_usd_tier_2
        WHEN ias_currency_code = 'USD'
        and IAS_USD_is_open = 1
        and CAST(IAS_USD_Current_Balance as double) >= 30000 then IAS_dynamic_rate_USD_tier_3
        WHEN ias_currency_code = 'EUR'
        and IAS_EUR_is_open = 1
        and CAST(IAS_EUR_Current_Balance as double) >= 3000
        and CAST(IAS_EUR_Current_Balance as double) < 10000 then IAS_dynamic_rate_EUR_tier_1
        WHEN ias_currency_code = 'EUR'
        and IAS_EUR_is_open = 1
        and CAST(IAS_EUR_Current_Balance as double) >= 10000
        and CAST(IAS_EUR_Current_Balance as double) < 30000 then IAS_dynamic_rate_EUR_tier_2
        WHEN ias_currency_code = 'EUR'
        and IAS_EUR_is_open = 1
        and CAST(IAS_EUR_Current_Balance as double) >= 30000 then IAS_dynamic_rate_EUR_tier_3
        ELSE 'Not Active'
    End As Customer_Active_Currency_Tier_Rate,
    CASE
        WHEN ias_currency_code = 'GBP'
        and IAS_GBP_is_open = 1
        and CAST(IAS_GBP_Current_Balance as double) >= 3000
        and CAST(IAS_GBP_Current_Balance as double) < 10000 then 'GBP Tier 1'
        WHEN ias_currency_code = 'GBP'
        and IAS_GBP_is_open = 1
        and CAST(IAS_GBP_Current_Balance as double) >= 10000
        and CAST(IAS_GBP_Current_Balance as double) < 25000 then 'GBP Tier 2'
        WHEN ias_currency_code = 'GBP'
        and IAS_GBP_is_open = 1
        and CAST(IAS_GBP_Current_Balance as double) >= 25000 then 'GBP Tier 3'
        WHEN ias_currency_code = 'USD'
        and IAS_USD_is_open = 1
        and CAST(IAS_USD_Current_Balance as double) >= 3000
        and CAST(IAS_USD_Current_Balance as double) < 10000 then 'USD Tier 1'
        WHEN ias_currency_code = 'USD'
        and IAS_USD_is_open = 1
        and CAST(IAS_USD_Current_Balance as double) >= 10000
        and CAST(IAS_USD_Current_Balance as double) < 30000 then 'USD Tier 2'
        WHEN ias_currency_code = 'USD'
        and IAS_USD_is_open = 1
        and CAST(IAS_USD_Current_Balance as double) >= 30000 then 'USD Tier 3'
        WHEN ias_currency_code = 'EUR'
        and IAS_EUR_is_open = 1
        and CAST(IAS_EUR_Current_Balance as double) >= 3000
        and CAST(IAS_EUR_Current_Balance as double) < 10000 then 'EUR Tier 1'
        WHEN ias_currency_code = 'EUR'
        and IAS_EUR_is_open = 1
        and CAST(IAS_EUR_Current_Balance as double) >= 10000
        and CAST(IAS_EUR_Current_Balance as double) < 30000 then 'EUR Tier 2'
        WHEN ias_currency_code = 'EUR'
        and IAS_EUR_is_open = 1
        and CAST(IAS_EUR_Current_Balance as double) >= 30000 then 'EUR Tier 3'
        ELSE 'Not Active'
    End As Customer_Active_Currency_Tier_Name,
    IAS_Open_Date,
    IAS_Account_State ---- IAS GBP ----
,
    IAS_GBP_is_Open,
    IAS_GBP_First_Deposit_Date,
    IAS_GBP_Last_Transaction_Date,
    IAS_GBP_Current_Balance ---- IAS USD ----
,
    IAS_USD_is_Open,
    IAS_USD_First_Deposit_Date,
    IAS_USD_Last_Transaction_Date,
    IAS_USD_Current_Balance ---- IAS USD ----
,
    IAS_EUR_is_Open,
    IAS_EUR_First_Deposit_Date,
    IAS_EUR_Last_Deposit_Date,
    IAS_EUR_Current_Balance ---- GBP ----
,
    IAS_Dynamic_Rate_GBP_Tier_1,
    IAS_Dynamic_Rate_GBP_Tier_2,
    IAS_Dynamic_Rate_GBP_Tier_3,
    IAS_Dynamic_Rate_GBP_Top_Rate ---- USD ----
,
    IAS_Dynamic_Rate_USD_Tier_1,
    IAS_Dynamic_Rate_USD_Tier_2,
    IAS_Dynamic_Rate_USD_Tier_3,
    IAS_Dynamic_Rate_USD_Top_Rate ---- EUR ----
,
    IAS_Dynamic_Rate_EUR_Tier_1,
    IAS_Dynamic_Rate_EUR_Tier_2,
    IAS_Dynamic_Rate_EUR_Tier_3,
    IAS_Dynamic_Rate_EUR_Top_Rate ---- General ----
,
    ia.IAS_No_Active_Accounts,
    ia.IAS_Has_Opened_Prev,
    c.Accrued_Interest_Amount as IAS_Interest_Accrued_Amount,
    b.ias_recurring_flag as IAS_Recurring_Transfer
from
    ias_aggregated as ia
    left join ias_rates as ir on ir.name = ia.ias_account_name
    left join ias_recurring as b on b.account_id = ia.ias_account_id
    left join latest_interest_accrued as c on c.parent_account_key = ia.ias_encoded_key
