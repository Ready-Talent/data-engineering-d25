{{ config(materialized='table') }}

SELECT
    payment_type,
    COUNT(order_id) AS total_orders,
    SUM(total_order_value) AS total_payment_value
FROM 
    {{ ref('olist_satar_fact') }}
GROUP BY 
    payment_type
ORDER BY 
    total_orders DESC
