With customers as (
SELECT dynamodb_keys_id_s, CASE WHEN dynamodb_new_image_card_ordered_bool = TRUE THEN 'YES' ELSE 'NO' END dynamodb_new_image_card_ordered_bool, dynamodb_new_image_status_s,
CONCAT(cast("year" AS VARCHAR), '-',
 cast("month" AS VARCHAR), '-',
 cast("day" AS VARCHAR)) AS date_column,
ROW_NUMBER() OVER(PARTITION BY dynamodb_keys_id_s
         ORDER BY dynamodb_new_image_updated_at_n DESC
                , timestamp_extracted DESC) rn
FROM datalake_curated.dynamo_scv_sls_customers
),

clients as (

                   Select id,
                   encoded_key,
                   preferred_language,
                   ROW_NUMBER() OVER (PARTITION BY id, encoded_key ORDER BY last_modified_date DESC) AS rn
                   From datalake_raw.clients
),

dep_accounts as (

                   Select encoded_key,
                   account_holder_key,
                   name,
                   coalesce(cast(balances_total_balance as decimal(18,2)), 0) as balances_total_balance,
                   ROW_NUMBER() OVER (PARTITION BY id ORDER BY last_modified_date DESC) AS rn
                   From datalake_raw.deposit_accounts
),

dep_transactions as (

                   Select parent_account_key,
                   booking_date,
                   ROW_NUMBER() OVER (PARTITION BY id ORDER BY timestamp_extracted DESC) AS rn,
                   ROW_NUMBER() OVER (PARTITION BY id ORDER BY timestamp_extracted) AS rn_first
                   From datalake_raw.deposit_transactions
                   Where type = 'DEPOSIT'
),

Cards_SLS as (


                   Select Date_format(COALESCE(dynamodb_new_image_updated_at_n, "date"), '%d/%m/%Y %h:%i:%s %p') AS slsd, dynamodb_new_image_user_id_s, dynamodb_new_image_token_n
                   ,ROW_NUMBER () OVER (PARTITION BY dynamo_sls_cards.dynamodb_keys_id_s
                        ORDER BY dynamo_sls_cards.dynamodb_new_image_updated_at_n,
                        dynamo_sls_cards.timestamp_extracted) AS rn
                   From datalake_raw.dynamo_sls_cards
                   Where dynamodb_new_image_state_s = 'ACTIVE'


),

Apple_Pay_Messages as (

                   Select dynamodb_new_image_token_n, MIN(dynamodb_new_image_updated_at_n) as First_APP_Trans_Date, MAX(dynamodb_new_image_updated_at_n) as Last_APP_Trans_Date
                   From datalake_curated.dynamo_card_fast_messages_default
                   Where dynamodb_new_image_message_m_iso_msg_m_de48_m_26_s = '103'
                   and dynamodb_new_image_message_m_summary_m_billing_amount_s >=0
                   and dynamodb_new_image_message_m_summary_m_processor_decision_desc_s IN ('Approve', 'Approve - NO CVV,NOT Chip,NOT off premesis=> DE39=00')
                   and dynamodb_new_image_message_m_message_type_m_message_desc_s IN ('Authorisation Request', 'Authorisation Advice')
                   and dynamodb_new_image_message_m_summary_m_spend_type_s IN ('POS - Purchase', 'ECOM - Purchase', 'ECOM - NA', 'POS - NA')
                   Group BY dynamodb_new_image_token_n


),

Cards_Fast_Messages as (

                   Select dynamodb_new_image_token_n, MIN(dynamodb_new_image_updated_at_n) as First_Card_Trans_Date, MAX(dynamodb_new_image_updated_at_n) as Last_Card_Trans_Date
                   From datalake_curated.dynamo_card_fast_messages_default
                   Where dynamodb_new_image_message_m_iso_msg_m_de48_m_26_s not in ('103')
                   and dynamodb_new_image_message_m_summary_m_billing_amount_s >=0
                   and dynamodb_new_image_message_m_summary_m_processor_decision_desc_s IN ('Approve', 'Approve - NO CVV,NOT Chip,NOT off premesis=> DE39=00')
                   and dynamodb_new_image_message_m_message_type_m_message_desc_s IN ('Authorisation Request', 'Authorisation Advice')
                   and dynamodb_new_image_message_m_summary_m_spend_type_s IN ('POS - Purchase', 'ECOM - Purchase', 'ECOM - NA', 'POS - NA', 'POS - Purchase with Cash Back')
                   Group BY dynamodb_new_image_token_n

),


Card_Orders as (


                   Select
		   Date_format(COALESCE(dynamodb_new_image_updated_at_n, "date"), '%d/%m/%Y %h:%i:%s %p') AS cod, Coalesce(dynamodb_new_image_user_id_s, dynamodb_keys_user_id_s) as zx
	           ,Row_Number () Over (Partition By dynamodb_new_image_user_id_s,
		          dynamodb_keys_user_id_s Order By dynamodb_new_image_created_at_n,
		           timestamp_extracted) AS rn
                   From datalake_raw.dynamo_sls_cardorders

),


pre_final as (
                   Select
	           c.dynamodb_keys_id_s,
                   fm.dynamodb_new_image_token_n,
                   c.dynamodb_new_image_card_ordered_bool,
                   cl.encoded_key,
                   cl.preferred_language,
                   da.account_holder_key,
                   da.name,
                   da.balances_total_balance,
                   date_format(from_iso8601_timestamp(dt.booking_date), '%d/%m/%Y %h:%i:%s %p') as inbound_payment_date,
                   dt.parent_account_key,
                   fm.First_APP_Trans_Date,
                   fm.Last_APP_Trans_Date,
                   fm1.First_Card_Trans_Date,
                   fm1.Last_Card_Trans_Date,
                   sl.slsd as Card_Activation_Date,
                   co.cod as Card_Ordered_Date,
	           CASE WHEN fm.dynamodb_new_image_token_n is NULL THEN 'NO' ELSE 'YES' END AS Cus_Has_Apple_Pay,
                   ROW_NUMBER() OVER (PARTITION BY c.dynamodb_keys_id_s, da.name order by dt.booking_date desc) AS rn
                   From customers c
                        LEFT JOIN clients cl
                           ON cl.id = c.dynamodb_keys_id_s
                           AND cl.rn = 1
                        LEFT JOIN dep_accounts da
                           ON da.account_holder_key = cl.encoded_key
                           AND da.rn = 1
                        LEFT JOIN dep_transactions dt
                           ON dt.parent_account_key = da.encoded_key
                           AND dt.rn = 1
                        LEFT JOIN Cards_SLS sl
                           ON sl.dynamodb_new_image_user_id_s = c.dynamodb_keys_id_s
                           AND sl.rn = 1
                        LEFT JOIN Apple_Pay_Messages fm
                           ON sl.dynamodb_new_image_token_n = fm.dynamodb_new_image_token_n
                           --AND fm.rn = 1
                        LEFT JOIN Cards_Fast_Messages fm1
                           ON sl.dynamodb_new_image_token_n = fm1.dynamodb_new_image_token_n
                           --AND fm1.rn = 1
                        LEFT JOIN Card_Orders co
                           ON co.zx = c.dynamodb_keys_id_s
                           AND co.rn = 1

                   Where c.rn = 1
                   and dynamodb_new_image_status_s = 'APPROVED'

),


pre_final_2 as (

                   Select
		   c.dynamodb_keys_id_s,
                   da.name,
                   date_format(DATE_PARSE(dt_first.booking_date, '%Y-%m-%dT%H:%i:%s.%f%z'), '%d/%m/%Y %h:%i:%s %p') as inbound_payment_date_first,
                   ROW_NUMBER() OVER (PARTITION BY c.dynamodb_keys_id_s, da.name order by dt_first.booking_date) AS rn
                   From customers c
                        LEFT JOIN clients cl
                           ON cl.id = c.dynamodb_keys_id_s
                           AND cl.rn = 1
                        LEFT JOIN dep_accounts da
                           ON da.account_holder_key = cl.encoded_key
                           AND da.rn = 1
                        LEFT JOIN dep_transactions dt_first
                           ON dt_first.parent_account_key = da.encoded_key
                           AND dt_first.rn_first = 1
                   Where c.rn = 1
                   and dynamodb_new_image_status_s = 'APPROVED'
)


, FINAL_SET_WITH_FIRST as (

      Select t1.dynamodb_keys_id_s
     ,max(case when t1.name = 'AED Current Account'
                then t1.inbound_payment_date_first
           end) AED_Current_Account_Last_Inbound_Payment_Date_first
     , max(case when t1.name = 'EUR Current Account'
                then t1.inbound_payment_date_first
           end) EUR_Current_Account_Last_Inbound_Payment_Date_first
     , max(case when t1.name = 'GBP Current Account'
                then t1.inbound_payment_date_first
           end) GBP_Current_Account_Last_Inbound_Payment_Date_first
     , max(case when t1.name = 'GBP Fixed Deposit Account Exclusive'
                then t1.inbound_payment_date_first
           end) GBP_Fixed_Deposit_Account_Exclusive_Last_Inbound_Payment_Date_first
     , max(case when t1.name = 'GBP Fixed Deposit Account Standard'
                then t1.inbound_payment_date_first
           end) GBP_Fixed_Deposit_Account_Standard_Last_Inbound_Payment_Date_first
     , max(case when t1.name = 'KWD Current Account'
                then t1.inbound_payment_date_first
           end) KWD_Current_Account_Last_Inbound_Payment_Date_first
     , max(case when t1.name = 'SAR Current Account'
                then t1.inbound_payment_date_first
           end) SAR_Current_Account_Last_Inbound_Payment_Date_first
     , max(case when t1.name = 'USD Current Account'
                then t1.inbound_payment_date_first
           end) USD_Current_Account_Last_Inbound_Payment_Date_first
     , max(case when t1.name = 'USD Fixed Deposit Account Exclusive'
                then t1.inbound_payment_date_first
           end) USD_Fixed_Deposit_Account_Exclusive_Last_Inbound_Payment_Date_first
     , max(case when t1.name = 'USD Fixed Deposit Account Standard'
                then t1.inbound_payment_date_first
           end) USD_Fixed_Deposit_Account_Standard_Last_Inbound_Payment_Date_first
     , max(case when t1.name not in ('AED Current Account', 'EUR Current Account',
                                 'GBP Current Account', 'GBP Fixed Deposit Account Exclusive',
                                 'GBP Fixed Deposit Account Standard', 'KWD Current Account',
                                 'SAR Current Account', 'USD Fixed Deposit Account Standard',
                                 'USD Fixed Deposit Account Exclusive', 'USD Current Account')
                then t1.inbound_payment_date_first
           end) as Other_Inbound_Payment_Date_first
From PRE_FINAL_2 t1
Where t1.rn = 1
Group By t1.dynamodb_keys_id_s


)


, agg_accounts as (
                   Select dynamodb_keys_id_s,
                   array_join(array_agg(name ORDER BY name DESC),', ') Account_Types
                   From pre_final
                   Where rn = 1
                   Group By dynamodb_keys_id_s
)


, final_set as(


Select t2.dynamodb_keys_id_s as Reference_ID
     , t1.dynamodb_new_image_card_ordered_bool AS Card_Ordered
     , t1.preferred_language
     , MIN(Date_format(t1.First_APP_Trans_Date, '%d/%m/%Y %h:%i:%s %p')) as First_APP_Trans_Date_a
     , MAX(Date_format(t1.Last_APP_Trans_Date, '%d/%m/%Y %h:%i:%s %p')) as Last_APP_Trans_Date_a
     , MIN(Date_format(t1.First_Card_Trans_Date, '%d/%m/%Y %h:%i:%s %p')) as First_Card_Trans_Date_a
     , MAX(Date_format(t1.Last_Card_Trans_Date, '%d/%m/%Y %h:%i:%s %p')) as Last_Card_Trans_Date_a
     , t1.Card_Activation_Date
     , t1.Card_Ordered_Date
     , t1.Cus_Has_Apple_Pay
     , t2.Account_Types
     , max(case when t1.name = 'AED Current Account'
                then t1.inbound_payment_date
           end) AED_Current_Account_Last_Inbound_Payment_Date
     , max(case when t1.name = 'AED Current Account'
                then t1.balances_total_balance
           end) AED_Current_Account_Total_Balance
     , max(case when t1.name = 'EUR Current Account'
                then t1.inbound_payment_date
           end) EUR_Current_Account_Last_Inbound_Payment_Date
     , max(case when t1.name = 'EUR Current Account'
                then t1.balances_total_balance
           end) EUR_Current_Account_Total_Balance
     , max(case when t1.name = 'GBP Current Account'
                then t1.inbound_payment_date
           end) GBP_Current_Account_Last_Inbound_Payment_Date
     , max(case when t1.name = 'GBP Current Account'
                then t1.balances_total_balance
           end) GBP_Current_Account_Total_Balance
     , max(case when t1.name = 'GBP Fixed Deposit Account Exclusive'
                then t1.inbound_payment_date
           end) GBP_Fixed_Deposit_Account_Exclusive_Last_Inbound_Payment_Date
     , max(case when t1.name = 'GBP Fixed Deposit Account Exclusive'
                then t1.balances_total_balance
           end) GBP_Fixed_Deposit_Account_Exclusive_Total_Balance
     , max(case when t1.name = 'GBP Fixed Deposit Account Standard'
                then t1.inbound_payment_date
           end) GBP_Fixed_Deposit_Account_Standard_Last_Inbound_Payment_Date
     , max(case when t1.name = 'GBP Fixed Deposit Account Standard'
                then t1.balances_total_balance
           end) GBP_Fixed_Deposit_Account_Standard_Total_Balance
     , max(case when t1.name = 'KWD Current Account'
                then t1.inbound_payment_date
           end) KWD_Current_Account_Last_Inbound_Payment_Date
     , max(case when t1.name = 'KWD Current Account'
                then t1.balances_total_balance
           end) KWD_Current_Account_Total_Balance
     , max(case when t1.name = 'SAR Current Account'
                then t1.inbound_payment_date
           end) SAR_Current_Account_Last_Inbound_Payment_Date
     , max(case when t1.name = 'SAR Current Account'
                then t1.balances_total_balance
           end) SAR_Current_Account_Total_Balance
     , max(case when t1.name = 'USD Current Account'
                then t1.inbound_payment_date
           end) USD_Current_Account_Last_Inbound_Payment_Date
     , max(case when t1.name = 'USD Current Account'
                then t1.balances_total_balance
           end) USD_Current_Account_Total_Balance
     , max(case when t1.name = 'USD Fixed Deposit Account Exclusive'
                then t1.inbound_payment_date
           end) USD_Fixed_Deposit_Account_Exclusive_Last_Inbound_Payment_Date
     , max(case when t1.name = 'USD Fixed Deposit Account Exclusive'
                then t1.balances_total_balance
           end) USD_Fixed_Deposit_Account_Exclusive_Total_Balance
     , max(case when t1.name = 'USD Fixed Deposit Account Standard'
                then t1.inbound_payment_date
           end) USD_Fixed_Deposit_Account_Standard_Last_Inbound_Payment_Date
     , max(case when t1.name = 'USD Fixed Deposit Account Standard'
                then t1.balances_total_balance
           end) USD_Fixed_Deposit_Account_Standard_Total_Balance
     , max(case when t1.name not in ('AED Current Account', 'EUR Current Account',
                                 'GBP Current Account', 'GBP Fixed Deposit Account Exclusive',
                                 'GBP Fixed Deposit Account Standard', 'KWD Current Account',
                                 'SAR Current Account', 'USD Fixed Deposit Account Standard',
                                 'USD Fixed Deposit Account Exclusive', 'USD Current Account')
                then t1.inbound_payment_date
           end) as Other_Inbound_Payment_Date
     , max(case when t1.name not in ('AED Current Account', 'EUR Current Account',
                                 'GBP Current Account', 'GBP Fixed Deposit Account Exclusive',
                                 'GBP Fixed Deposit Account Standard', 'KWD Current Account',
                                 'SAR Current Account', 'USD Fixed Deposit Account Standard',
                                 'USD Fixed Deposit Account Exclusive', 'USD Current Account')
                then t1.balances_total_balance
           end) as Other_Balances_Total_Balance
From PRE_FINAL t1
       INNER JOIN agg_accounts t2
               ON t1.dynamodb_keys_id_s = t2.dynamodb_keys_id_s
Where t1.rn = 1
--AND t1.balances_total_balance is not null
Group By t2.dynamodb_keys_id_s, t1.dynamodb_new_image_card_ordered_bool, t1.preferred_language
     , t1.First_APP_Trans_Date, t1.Last_APP_Trans_Date, t1.First_Card_Trans_Date, t1.Last_Card_Trans_Date, t1.Card_Activation_Date,
     t1.Card_Ordered_Date, t2.Account_Types, t1.Cus_Has_Apple_Pay
)


Select t1.Reference_ID
     , max(t1.Card_Ordered                                                       ) as Card_Ordered
     , max(t1.preferred_language                                                 ) as preferred_language
     , min(t1.First_APP_Trans_Date_a                                             ) as First_APP_Trans_Date
     , max(t1.Last_APP_Trans_Date_a                                              ) as Last_APP_Trans_Date
     , min(t1.First_Card_Trans_Date_a                                            ) as First_Card_Trans_Date
     , max(t1.Last_Card_Trans_Date_a                                             ) as Last_Card_Trans_Date
     , max(t1.Card_Activation_Date                                               ) as Card_Activation_Date
     , max(t1.Card_Ordered_Date                                                  ) as Card_Ordered_Date
     , max(t1.Cus_Has_Apple_Pay                                                  ) as Cus_Has_Apple_Pay
     , max(t1.Account_Types                                                      ) as Account_Types
     , max(t1.AED_Current_Account_Last_Inbound_Payment_Date                      ) as AED_Current_Account_Last_Inbound_Payment_Date
     , max(t1.AED_Current_Account_Total_Balance                                  ) as AED_Current_Account_Total_Balance
     , max(t1.EUR_Current_Account_Last_Inbound_Payment_Date                      ) as EUR_Current_Account_Last_Inbound_Payment_Date
     , max(t1.EUR_Current_Account_Total_Balance                                  ) as EUR_Current_Account_Total_Balance
     , max(t1.GBP_Current_Account_Last_Inbound_Payment_Date                      ) as GBP_Current_Account_Last_Inbound_Payment_Date
     , max(t1.GBP_Current_Account_Total_Balance                                  ) as GBP_Current_Account_Total_Balance
     , max(t1.GBP_Fixed_Deposit_Account_Exclusive_Last_Inbound_Payment_Date      ) as GBP_Fixed_Deposit_Account_Exclusive_Last_Inbound_Payment_Date
     , max(t1.GBP_Fixed_Deposit_Account_Exclusive_Total_Balance                  ) as GBP_Fixed_Deposit_Account_Exclusive_Total_Balance
     , max(t1.GBP_Fixed_Deposit_Account_Standard_Last_Inbound_Payment_Date       ) as GBP_Fixed_Deposit_Account_Standard_Last_Inbound_Payment_Date
     , max(t1.GBP_Fixed_Deposit_Account_Standard_Total_Balance                   ) as GBP_Fixed_Deposit_Account_Standard_Total_Balance
     , max(t1.KWD_Current_Account_Last_Inbound_Payment_Date                      ) as KWD_Current_Account_Last_Inbound_Payment_Date
     , max(t1.KWD_Current_Account_Total_Balance                                  ) as KWD_Current_Account_Total_Balance
     , max(t1.SAR_Current_Account_Last_Inbound_Payment_Date                      ) as SAR_Current_Account_Last_Inbound_Payment_Date
     , max(t1.SAR_Current_Account_Total_Balance                                  ) as SAR_Current_Account_Total_Balance
     , max(t1.USD_Current_Account_Last_Inbound_Payment_Date                      ) as USD_Current_Account_Last_Inbound_Payment_Date
     , max(t1.USD_Current_Account_Total_Balance                                  ) as USD_Current_Account_Total_Balance
     , max(t1.USD_Fixed_Deposit_Account_Exclusive_Last_Inbound_Payment_Date      ) as USD_Fixed_Deposit_Account_Exclusive_Last_Inbound_Payment_Date
     , max(t1.USD_Fixed_Deposit_Account_Exclusive_Total_Balance                  ) as USD_Fixed_Deposit_Account_Exclusive_Total_Balance
     , max(t1.USD_Fixed_Deposit_Account_Standard_Last_Inbound_Payment_Date       ) as USD_Fixed_Deposit_Account_Standard_Last_Inbound_Payment_Date
     , max(t1.USD_Fixed_Deposit_Account_Standard_Total_Balance                   ) as USD_Fixed_Deposit_Account_Standard_Total_Balance
     , max(t1.Other_Inbound_Payment_Date                                         ) as Other_Inbound_Payment_Date
     , max(t1.Other_Balances_Total_Balance                                       ) as Other_Balances_Total_Balance
     , max(t2.AED_Current_Account_Last_Inbound_Payment_Date_first                ) as AED_Current_Account_First_Inbound_Payment_Date
     , max(t2.EUR_Current_Account_Last_Inbound_Payment_Date_first                ) as EUR_Current_Account_First_Inbound_Payment_Date
     , max(t2.GBP_Current_Account_Last_Inbound_Payment_Date_first                ) as GBP_Current_Account_First_Inbound_Payment_Date
     , max(t2.GBP_Fixed_Deposit_Account_Exclusive_Last_Inbound_Payment_Date_first) as GBP_Fixed_Deposit_Account_Exclusive_First_Inbound_Payment_Date
     , max(t2.GBP_Fixed_Deposit_Account_Standard_Last_Inbound_Payment_Date_first ) as GBP_Fixed_Deposit_Account_Standard_First_Inbound_Payment_Date
     , max(t2.KWD_Current_Account_Last_Inbound_Payment_Date_first                ) as KWD_Current_Account_First_Inbound_Payment_Date
     , max(t2.SAR_Current_Account_Last_Inbound_Payment_Date_first                ) as SAR_Current_Account_First_Inbound_Payment_Date
     , max(t2.USD_Current_Account_Last_Inbound_Payment_Date_first                ) as USD_Current_Account_First_Inbound_Payment_Date
     , max(t2.USD_Fixed_Deposit_Account_Exclusive_Last_Inbound_Payment_Date_first) as USD_Fixed_Deposit_Account_Exclusive_First_Inbound_Payment_Date
     , max(t2.USD_Fixed_Deposit_Account_Standard_Last_Inbound_Payment_Date_first ) as USD_Fixed_Deposit_Account_Standard_First_Inbound_Payment_Date
     , max(t2.Other_Inbound_Payment_Date_first                                   ) as Other_First_Inbound_Payment_Date

 From FINAL_SET t1
       LEFT JOIN FINAL_SET_WITH_FIRST t2
       on t1.Reference_ID = t2.dynamodb_keys_id_s
 Where 1=1
 group by t1.Reference_ID
 order by 1
