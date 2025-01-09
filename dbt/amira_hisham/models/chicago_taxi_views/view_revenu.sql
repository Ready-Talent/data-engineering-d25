WITH monthly_revenue AS (
    SELECT
        EXTRACT(YEAR FROM ft.trip_start_timestamp) AS year,
        EXTRACT(MONTH FROM ft.trip_start_timestamp) AS month,
        SUM(df.trip_total) AS total_revenue
    FROM
        {{ ref('fact_trip') }} ft
    LEFT JOIN
        {{ ref('dim_trip_fees') }} df ON ft.unique_key = df.unique_key
    GROUP BY
        year, month
)
SELECT
    year,
    month,
    total_revenue
FROM
    monthly_revenue
ORDER BY
    year DESC, month DESC
