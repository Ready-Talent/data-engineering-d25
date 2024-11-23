{{ config(
    materialized='incremental',
    unique_key='unique_key'
) }}


WITH date_range AS (
  SELECT
    min( extract(date from trip_start_timestamp)) AS start_date,
    max( extract(date from trip_start_timestamp)) AS end_date
  from ready-de-25.landing.chicago_taxi_trips
),
generated_dates AS (
  SELECT
    date
  FROM
    UNNEST(GENERATE_DATE_ARRAY(
      (SELECT start_date FROM date_range),
      (SELECT end_date FROM date_range),
      INTERVAL 1 DAY
    )) AS date
)
SELECT
  date AS date,
  EXTRACT(YEAR FROM date) AS year,
  EXTRACT(MONTH FROM date) AS month,
  EXTRACT(day FROM date) AS day
FROM
  generated_dates