WITH location_counts AS (
    SELECT
        dl.pickup_location AS pickup_location,
        dl.dropoff_location AS dropoff_location,
        COUNT(*) AS trip_count
    FROM
        {{ ref('fact_trip') }} ft
    LEFT JOIN
        {{ ref('dim_location') }} dl ON ft.unique_key = dl.unique_key
    GROUP BY
        pickup_location, dropoff_location
)
SELECT
    pickup_location,
    dropoff_location,
    trip_count
FROM
    location_counts
ORDER BY
    trip_count DESC
LIMIT 10
