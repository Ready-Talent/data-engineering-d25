{{ config(materialized='table') }}

WITH timestamps AS (
    SELECT DISTINCT trip_start_timestamp AS timestamp
    FROM ready-de-25.landing.chicago_taxi_trips
    
    UNION DISTINCT  -- Ensures uniqueness
    
    SELECT DISTINCT trip_end_timestamp AS timestamp
    FROM ready-de-25.landing.chicago_taxi_trips
)
SELECT 
    ROW_NUMBER() OVER () AS time_id,
    timestamp,
    EXTRACT(YEAR FROM timestamp) AS year,
    EXTRACT(MONTH FROM timestamp) AS month,
    EXTRACT(DAY FROM timestamp) AS day,
    EXTRACT(HOUR FROM timestamp) AS hour
FROM timestamps