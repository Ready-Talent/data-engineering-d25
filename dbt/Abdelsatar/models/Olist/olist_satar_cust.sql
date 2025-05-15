{{ config(materialized='table') }}

SELECT
    customer_id,
    SUM(total_order_value) AS total_order_value
FROM 
    {{ ref('olist_satar_fact') }}
GROUP BY 
    customer_id
ORDER BY 
    total_order_value DESC
LIMIT 10
