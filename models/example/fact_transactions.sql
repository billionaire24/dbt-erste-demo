{{ config(materialized='table') }}

with raw_transactions as (

select 
ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS transaction_id,
b.counterparty_id,
c.product_id,   
d.merchant_id,
a.transaction_id as bk_transaction_id,
a.client_num,
cast(a.transaction_amount as decimal) transaction_amount,
cast(a.transaction_date as date) transaction_date,
a.status

from
{{ source('raw', 'raw_card_operations') }} a 
LEFT JOIN {{ ref('dim_customers') }}  b on a.client_num = b.client_number
LEFT JOIN {{ ref('fact_applications') }}  c on b.counterparty_id = c.counterparty_id
LEFT JOIN {{ ref('dim_merchant') }}  d on a.merchant = d.merchant

)

select *

from
raw_transactions