{{
    config(
        materialized='incremental',
        unique_key='unique_key'
    )
}}

SELECT unique_key, pickup_census_tract, pickup_community_area, pickup_latitude, pickup_longitude,
pickup_location FROM ready-de-25.landing.chicago_taxi_trips
