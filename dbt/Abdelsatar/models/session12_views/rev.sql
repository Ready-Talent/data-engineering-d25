-- models/total_revenue_per_month.sql
{{ config(materialized='view') }}

SELECT 
    EXTRACT(YEAR FROM trip_start_timestamp) AS year,
    EXTRACT(MONTH FROM trip_start_timestamp) AS month,
    SUM(fare + tips + tolls + extras) AS total_revenue
FROM ready-de-25.landing.chicago_taxi_trips
GROUP BY year, month
ORDER BY year, month;