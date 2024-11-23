{{
    config(
        materialized='incremental',
        unique_key='customer_id'
    )
}}

SELECT customer_id, name, email, phone, address FROM ready-de-25.ecommerce.customers