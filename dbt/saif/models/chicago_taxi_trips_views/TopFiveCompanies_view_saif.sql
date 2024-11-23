SELECT
    company,
    COUNT(DISTINCT taxi_id) AS taxis
FROM
    `ready-de-25.dbt_samir.fact_trip_saif` WHERE company IS NOT NULL
GROUP BY
    company
ORDER BY
    taxis DESC
LIMIT 5