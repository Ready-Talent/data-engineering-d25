{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

SELECT
    o.order_id,
    o.order_date,
    c.customer_id
FROM `ready-de-25.ecommerce.orders` o
JOIN {{ ref('dim_customer_amira_dbt') }} c ON o.customer_id = c.customer_id