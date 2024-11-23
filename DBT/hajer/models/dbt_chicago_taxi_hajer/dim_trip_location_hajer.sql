{{ config(
    materialized='incremental',
    unique_key='unique_key'
) }}


SELECT unique_key, pickup_location, dropoff_location
FROM ready-de-25.landing.chicago_taxi_trips