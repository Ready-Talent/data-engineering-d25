SELECT
    FORMAT_TIMESTAMP('%Y-%m', trip_start_timestamp) AS month,
    SUM(trip_total) AS total_revenue
FROM
    `ready-de-25.dbt_samir.fact_trip_saif`
GROUP BY
    month
ORDER BY
    month