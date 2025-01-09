{{
config(
    materialized = 'view',
)
}}

select unique_key,pickup_location, dropoff_location
from {{ ref('Fact_Taxi') }} 
