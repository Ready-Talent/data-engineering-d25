{{
    config(
        materialized='incremental',
        unique_key='unique_key'
    )
}}

SELECT unique_key, payment_type, fare, tips, tolls, extras
FROM ready-de-25.landing.chicago_taxi_trips
