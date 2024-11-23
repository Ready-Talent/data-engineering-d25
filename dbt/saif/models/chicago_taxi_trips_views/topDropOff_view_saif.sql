SELECT 
    dropoff_location AS location, 
    COUNT(*) AS total_dropoffs
FROM 
    `ready-de-25.dbt_samir.dim_location_saif` WHERE dropoff_location IS NOT NULL
GROUP BY 
    dropoff_location
ORDER BY 
    total_dropoffs DESC
LIMIT 10