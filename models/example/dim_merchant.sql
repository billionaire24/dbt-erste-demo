{{ config(materialized='table') }}

with dane as (

    select distinct
    ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS merchant_id, 
    merchant
    from
    (
    select distinct
    merchant
    from
    {{ source('raw', 'raw_card_operations') }} 

    )
)

select *
from dane   