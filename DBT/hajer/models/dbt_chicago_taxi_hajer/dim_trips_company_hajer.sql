{{ config(
    materialized='incremental',
    unique_key='unique_key'
) }}


SELECT unique_key, taxi_id, company
FROM ready-de-25.landing.chicago_taxi_trips