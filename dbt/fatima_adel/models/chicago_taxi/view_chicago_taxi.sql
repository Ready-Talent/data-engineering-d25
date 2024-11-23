{{
    config(
        materialized='view'
        
    )
}}
SELECT unique_key, taxi_id, company, trip_start_timestamp, trip_end_timestamp, trip_miles, fare, trip_total,
pickup_location, dropoff_location, payment_type
FROM ready-de-25.landing.chicago_taxi_trips