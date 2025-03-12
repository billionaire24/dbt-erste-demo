{{ config(materialized='table') }}

with raw_application as (

select 
    ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS  application_id,
    b.counterparty_id,
    c.product_id,  
    a.application_number,							
    a.application_date,				
    a.status,				
    a.credit_card_type, 		
    cast(a.agreement_assignment_date as date) agreement_assignment_date,				
    cast(a.decision_date as date) decision_date	,			
    a.branch
    from
    {{ source('raw', 'raw_client_applications') }} a 
    LEFT JOIN {{ ref('dim_customers') }} b on a.client_number = b.client_number
    LEFT JOIN {{ ref('dim_product') }}  c on a.credit_card_type = c.type

)

select *

from
raw_application