{{ config(materialized='table') }}

SELECT DISTINCT
    ROW_NUMBER() OVER () AS payment_type_id,
    payment_type
FROM ready-de-25.landing.chicago_taxi_trips;
