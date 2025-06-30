{{ 
    config(
             materialized='ephemeral',
    ) 
}}

with source as (
    select
        id
        , order_id
        , product_id
        , customer_id
        , quantity
        , price
        , to_date(sale_date) as sale_date
        , updated_at
    from {{ source('snowflake_source','sales') }}  
)
select *
from source