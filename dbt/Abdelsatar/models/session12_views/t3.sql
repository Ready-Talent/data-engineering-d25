{{ config(materialized='view') }}

SELECT 
    payment_type,
    COUNT(*) AS payment_type_count,
    (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM ready-de-25.landing.chicago_taxi_trips)) AS payment_type_percentage
FROM ready-de-25.landing.chicago_taxi_trips
GROUP BY payment_type
ORDER BY payment_type_count DESC