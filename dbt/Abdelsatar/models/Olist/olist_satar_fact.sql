{{ config(materialized='table') }}
  
SELECT 
    o.order_id,                         
    o.customer_id,                      
    oi.product_id,                      
    pc.product_category_name AS category_name,  
    p.payment_type,                     
    DATE_TRUNC(DATE(CAST(o.order_purchase_timestamp AS TIMESTAMP)), MONTH) AS order_month,  
    COUNT(o.order_id) OVER (PARTITION BY o.customer_id) AS total_orders_per_customer,  
    SUM(oi.price) AS total_order_value, 
    SUM(oi.freight_value) AS total_freight_value, 
    SUM(oi.order_item_id) AS total_quantity,      
    --SUM(oi.price) OVER (PARTITION BY pc.product_category_name) AS category_revenue  
    SUM(oi.price) AS category_revenue  
FROM 
    `ready-de-25.olist_abdelsatar.orders` o
JOIN 
    `ready-de-25.olist_abdelsatar.order_items` oi ON o.order_id = oi.order_id
JOIN 
    `ready-de-25.olist_abdelsatar.order_payments` p ON o.order_id = p.order_id
JOIN 
    `ready-de-25.olist_abdelsatar.products` pc ON oi.product_id = pc.product_id
GROUP BY 
    o.order_id, o.customer_id, oi.product_id, pc.product_category_name, p.payment_type, order_month 