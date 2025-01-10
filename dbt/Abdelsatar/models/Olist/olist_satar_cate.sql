{{ config(materialized='table') }}

SELECT
    t.string_field_1 AS category_name_en,  
    SUM(f.category_revenue) AS total_revenue
FROM 
    {{ ref('olist_satar_fact') }} AS f  
JOIN 
    `ready-de-25.olist_abdelsatar.product_category_name_translation` AS t   
ON 
    f.category_name = t.string_field_0  
GROUP BY 
    t.string_field_1
ORDER BY 
    total_revenue DESC
LIMIT 10
