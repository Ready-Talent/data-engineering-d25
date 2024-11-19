{{ config(
    materialized='incremental',
    unique_key='customer_id'
) }}

select *
from ready-de-25.ecommerce.customers