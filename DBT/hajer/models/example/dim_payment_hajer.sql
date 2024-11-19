{{ config(
    materialized='incremental',
    unique_key='payment_id'
) }}

select *
from ready-de-25.ecommerce.payment