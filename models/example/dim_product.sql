{{ config(materialized='table') }}

with raw_products as (
    select 
    ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS product_id,
    type,
    credit_limit,
    annual_fee

    from
    {{ source('raw', 'raw_products') }} a
)

select *

from
raw_products