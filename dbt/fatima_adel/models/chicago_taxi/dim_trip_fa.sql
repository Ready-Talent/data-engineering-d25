{{
    config(
        materialized='incremental',
        unique_key='unique_key'
    )
}}

SELECT unique_key, trip_start_timestamp, trip_end_timestamp, trip_seconds
FROM ready-de-25.landing.chicago_taxi_trips
