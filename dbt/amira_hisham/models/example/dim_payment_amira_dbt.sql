{{ config(
    materialized='incremental',
    unique_key='payment_id'
) }}
SELECT     
    p.payment_id,
    p.amount ,
    p.payment_date 

FROM `ready-de-25.ecommerce.payment` p