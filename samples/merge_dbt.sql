{{ config(
    materialized='incremental',
    unique_key='id'
) }}

SELECT
    id,
    column1,
    column2
FROM {{ ref('staging_table') }}

{% if is_incremental() %}
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
