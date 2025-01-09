{{ config(
    materialized='incremental',
    unique_key='id'
) }}

SELECT *
FROM project_id.dataset.table_name

{% if is_incremental() %}
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
