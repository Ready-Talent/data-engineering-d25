{{ config(materialized="view") }}

select
    extract(year from trip_start_timestamp) as year,
    extract(month from trip_start_timestamp) as month,
    sum(fare + tips + tolls + extras) as total_revenue
from {{ ref('Fact_Taxi') }} 
group by year, month
order by year, month
