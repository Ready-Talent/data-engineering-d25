{{ config(
    materialized='incremental',
    unique_key='unique_key'
) }}

SELECT    
    unique_key,
    fare,
    tips,
    trip_total,
    extras,
    payment_type
FROM `ready-de-25.landing.chicago_taxi_trips`