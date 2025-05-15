{{ config(materialized='table') }}

SELECT
    customer_id,
    AVG(total_orders_per_customer) AS avg_orders_per_customer
FROM 
    {{ ref('olist_satar_fact') }}
GROUP BY 
    customer_id
