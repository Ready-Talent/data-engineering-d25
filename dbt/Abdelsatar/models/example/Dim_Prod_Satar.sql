{{ config(
    materialized='incremental',
    unique_key='product_id'
) }}


SELECT * 
FROM `ready-de-25.ecommerce.products`