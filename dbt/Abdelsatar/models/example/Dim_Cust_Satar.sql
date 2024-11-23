{{ config(
    materialized='incremental',
    unique_key='customer_id'
) }}


SELECT * 
FROM `ready-de-25.ecommerce.customers`
