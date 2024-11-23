
SELECT 
    pickup_location AS location, 
    COUNT(*) AS total_pickups
FROM 
    `ready-de-25.dbt_samir.dim_location_saif` WHERE pickup_location IS NOT NULL
GROUP BY 
    pickup_location
ORDER BY 
    total_pickups DESC
LIMIT 10

 

