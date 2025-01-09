WITH company_taxi_counts AS (
    SELECT
        ft.company,
        COUNT(DISTINCT ft.taxi_id) AS taxi_count
    FROM
        {{ ref('fact_trip') }} ft
    GROUP BY
        company
)
SELECT
    company,
    taxi_count
FROM
    company_taxi_counts
ORDER BY
    taxi_count DESC
LIMIT 5
