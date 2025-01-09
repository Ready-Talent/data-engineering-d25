WITH payment_type_counts AS (
    SELECT
        df.payment_type,
        COUNT(*) AS payment_count
    FROM
        {{ ref('dim_trip_fees') }} df
    GROUP BY
        payment_type
)
SELECT
    pt.payment_type,
    pt.payment_count,
    (pt.payment_count / (SELECT COUNT(*) FROM {{ ref('dim_trip_fees') }} df)) * 100 AS percentage
FROM
    payment_type_counts pt
ORDER BY
    percentage DESC
