{{ config(
    materialized='incremental',
    unique_key='customer_id'
) }}

SELECT    
    c.customer_id,
    c.name,
    c.address ,
    c.email ,
    c.phone

FROM `ready-de-25.ecommerce.customers` c