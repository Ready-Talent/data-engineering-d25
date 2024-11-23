{{ config(
    materialized='incremental',
    unique_key='unique_key'
) }}


SELECT unique_key, trip_total, trip_miles
FROM ready-de-25.landing.chicago_taxi_trips