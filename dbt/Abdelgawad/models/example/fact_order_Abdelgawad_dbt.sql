{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

SELECT 
    o.order_id,
    o.customer_id,
    DATE(o.order_date),
    op.product_id,
    op.quantity,
    op.total_price,
    p.payment_id,
    DATE(o.created_at_timestamp),
    DATE(o.updated_at_timestamp)
FROM ecommerce.orders AS o
JOIN ecommerce.orders_products AS op ON o.order_id = op.order_id
LEFT JOIN ecommerce.payment AS p ON p.order_detail_id = op.order_id