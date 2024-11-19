SELECT
    id,
    SUM(CASE WHEN country = 'US' THEN revenue ELSE 0 END) AS us_revenue,
    SUM(CASE WHEN country = 'CA' THEN revenue ELSE 0 END) AS ca_revenue
FROM sales_data
GROUP BY id;
