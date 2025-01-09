-- models/top_locations.sql
{{ config(materialized='view') }}

WITH pickup_locations AS (
    SELECT pickup_location AS location
    FROM ready-de-25.landing.chicago_taxi_trips
),
dropoff_locations AS (
    SELECT dropoff_location AS location
    FROM ready-de-25.landing.chicago_taxi_trips
)

SELECT 
    location,
    location_type,
    COUNT(*) AS location_count
FROM (
    -- Pickup locations
    SELECT pickup_location AS location, 'pickup' AS location_type
    FROM ready-de-25.landing.chicago_taxi_trips

    UNION ALL

    -- Dropoff locations
    SELECT dropoff_location AS location, 'dropoff' AS location_type
    FROM ready-de-25.landing.chicago_taxi_trips
)
GROUP BY location, location_type
ORDER BY location_count DESC