{{ config(materialized='table') }}

SELECT DISTINCT
    ROW_NUMBER() OVER () AS company_id,
    company AS company_name
FROM ready-de-25.landing.chicago_taxi_trips;
