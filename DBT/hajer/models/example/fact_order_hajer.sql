{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

SELECT o.order_id, c.customer_id,py.payment_id,p.product_id,o.order_date
FROM ready-de-25.ecommerce.orders o
join {{ ref('dim_customer_hajer') }} c on o.customer_id = c.customer_id
join ready-de-25.ecommerce.orders_products op on o.order_id = op.order_id
join {{ ref('dim_product_hajer') }} p on p.product_id = op.product_id
join ready-de-25.ecommerce.payment py on o.order_id = py.order_detail_id
