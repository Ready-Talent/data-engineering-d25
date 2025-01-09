{{ config(
    materialized='incremental',
    unique_key='product_id'
) }}

select *
from ready-de-25.ecommerce.products