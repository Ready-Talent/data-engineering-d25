{{ config(materialized='table') }}

SELECT
    product_id,
    SUM(total_quantity) AS total_quantity
FROM 
    {{ ref('olist_satar_fact') }}
GROUP BY 
    product_id
ORDER BY 
    total_quantity DESC
LIMIT 10
