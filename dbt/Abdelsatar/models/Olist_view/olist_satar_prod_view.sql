{{ config(materialized='view') }}

SELECT * 
FROM {{ ref('olist_satar_prod') }}
