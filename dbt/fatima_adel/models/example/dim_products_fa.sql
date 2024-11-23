{{
    config(
        materialized='incremental',
        unique_key='product_id'
    )
}}

SELECT product_id, name, description, price  FROM ready-de-25.ecommerce.products