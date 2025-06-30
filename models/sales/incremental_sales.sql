{{ config(
        materialized='incremental',
        unique_key='order_id',
        alias='incr_sales'
    ) 
}}

with incremental_source as (
    select
        id
        , order_id
        , product_id
        , customer_id
        , quantity
        , price
        , sale_date
        , updated_at
        , CURRENT_TIMESTAMP as load_date
    from
        {{ ref('stg_sales') }}
    {% if is_incremental() %}
        where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)
select * 
from
    incremental_source





