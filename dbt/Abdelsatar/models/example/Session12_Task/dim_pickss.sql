{{ config(materialized='table') }}

SELECT DISTINCT
    ROW_NUMBER() OVER () AS locations_id,
    dropoff_location AS Dropoffs,
    pickoff_location AS Pickups
FROM ready-de-25.landing.chicago_taxi_trips;