{{ config(
    materialized='incremental',
    unique_key='unique_key'
) }}

SELECT    
    unique_key,
    pickup_location,
    dropoff_location,
    pickup_census_tract,
    dropoff_census_tract,
    pickup_community_area,
    dropoff_community_area,
    pickup_latitude,
    pickup_longitude,
    dropoff_latitude,
    dropoff_longitude
FROM `ready-de-25.landing.chicago_taxi_trips`