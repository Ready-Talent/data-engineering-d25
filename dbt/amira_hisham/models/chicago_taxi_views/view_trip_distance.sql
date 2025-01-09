WITH trip_length_counts AS (
    SELECT
        CASE
            WHEN ft.trip_miles < 5 THEN 'Short Trip (<5 miles)'
            ELSE 'Long Trip (>=5 miles)'
        END AS trip_type,
        COUNT(*) AS trip_count
    FROM
        {{ ref('fact_trip') }} ft
    GROUP BY
        trip_type
)
SELECT
    trip_type,
    trip_count
FROM
    trip_length_counts
