{{ config(
    materialized='incremental',
    unique_key='unique_key'
) }}


SELECT * 
FROM `ready-de-25.landing.chicago_taxi_trips`