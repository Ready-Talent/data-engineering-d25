SELECT
    CASE 
        WHEN trip_miles < 5 THEN 'Short (Under 5 miles)'
        ELSE 'Long (5 miles or more)'
    END AS trip_category,
    ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()), 2) AS percentage
FROM
    `ready-de-25.dbt_samir.fact_trip_saif`
GROUP BY
    trip_category
ORDER BY
    trip_category