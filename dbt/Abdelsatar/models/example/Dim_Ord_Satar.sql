{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}


SELECT * 
FROM `ready-de-25.ecommerce.orders`