{{ config(
    materialized='incremental',
    unique_key='unique_key'
) }}

SELECT unique_key, fare, tips, tolls, extras, trip_total, payment_type
FROM ready-de-25.landing.chicago_taxi_trips