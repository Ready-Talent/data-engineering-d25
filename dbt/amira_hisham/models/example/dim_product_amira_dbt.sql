{{ config(
    materialized='incremental',
    unique_key='product_id'
) }}
SELECT     
    pr.product_id,
    pr.price ,
    pr.name 

FROM `ready-de-25.ecommerce.products` pr