{{
    config(
        materialized='incremental',
        unique_key='order_id'
    )
}}

SELECT op.order_id, op.product_id, py.payment_id, o.customer_id, o.order_date, op.quantity, op.total_price
  FROM ready-de-25.ecommerce.orders_products op 
  JOIN ready-de-25.ecommerce.orders o ON op.order_id = o.order_id
  JOIN {{ ref('dim_payment_fa') }} py ON op.order_id = py.order_detail_id
