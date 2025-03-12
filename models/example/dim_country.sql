{{ config(materialized='table') }}

with raw_region as (

    SELECT 
    ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS region_id,
    city,
    state,
    postcode
    FROM 
    {{ source('raw', 'raw_clients') }} a

)

select *

from raw_region
