SELECT 
    customer_id,
    name,
    email,
    address,
    phone,
    DATE(created_at_timestamp),
    DATE(updated_at_timestamp)
FROM ecommerce.customers;
