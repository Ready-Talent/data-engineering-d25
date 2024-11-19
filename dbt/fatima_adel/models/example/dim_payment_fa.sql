{{
    config(
        materialized='incremental',
        unique_key='payment_id'
    )
}}
SELECT payment_id, order_detail_id, payment_date, amount  FROM ready-de-25.ecommerce.payment 

