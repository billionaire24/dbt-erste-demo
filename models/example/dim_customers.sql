{{ config(materialized='table') }}

with raw_clients as (
    select 
    ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS counterparty_id,
    b.region_id, 
    client_number,
    Upper(name) name,				
    email,			
    phone_number,
    bulding_number,
    street_name,
    cast(birth_date as date) birth_date,
    DATE_DIFF(birth_date, CURRENT_DATE, DAY) as Age
    from
    {{ source('raw', 'raw_clients') }} a 
    LEFT JOIN {{ ref('dim_country') }}  b on a.postcode = b.postcode
    and a.city = b.city
    and a.state = b.state
)

select *

from
raw_clients