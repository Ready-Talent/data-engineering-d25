{{ config(
    materialized='incremental',
    unique_key='payment_id'
) }}


SELECT * 
FROM `ready-de-25.ecommerce.payment`