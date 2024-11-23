{{
    config(
        materialized='incremental',
        unique_key='unique_key'
    )
}}

SELECT unique_key, dropoff_census_tract, dropoff_community_area, dropoff_latitude, dropoff_longitude,
dropoff_location FROM ready-de-25.landing.chicago_taxi_trips
