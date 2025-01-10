{{ config(materialized='table') }}

SELECT
    order_month,
    COUNT(order_id) AS total_orders
FROM 
    {{ ref('olist_satar_fact') }}
GROUP BY 
    order_month
ORDER BY 
    order_month
