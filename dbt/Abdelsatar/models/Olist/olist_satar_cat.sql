{{ config(materialized='table') }}

SELECT
    category_name,
    SUM(category_revenue) AS total_revenue
FROM 
    {{ ref('olist_satar_fact') }}
GROUP BY 
    category_name
ORDER BY 
    total_revenue DESC
LIMIT 10
