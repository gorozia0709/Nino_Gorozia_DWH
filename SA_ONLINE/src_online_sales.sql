CREATE TABLE IF NOT EXISTS sa_online_sales.src_online_sales (
    store_id VARCHAR(255),
    store_name VARCHAR(255),
    store_website VARCHAR(255),
    supplier_id VARCHAR(255),
    supplier VARCHAR(255),
    supplier_email VARCHAR(255),
    contract_start VARCHAR(255),
    contract_end VARCHAR(255),
    product_id VARCHAR(255),
    product_name VARCHAR(255),
    brand VARCHAR(255),
    category VARCHAR(255),
    subcategory VARCHAR(255),
    rating VARCHAR(255),
    price VARCHAR(255),
    transaction_id VARCHAR(255),
    date VARCHAR(255),
    quantity_sold VARCHAR(255),
    amount_sold VARCHAR(255),
    cost_amount VARCHAR(255),
    payment_method VARCHAR(255),
    shipping_address VARCHAR(255),
    shipping_price VARCHAR(255),
    shipping_state VARCHAR(255),
    shipping_city VARCHAR(255),
	shipping_method VARCHAR(255),
    customer_id VARCHAR(255),
    customer_firstname VARCHAR(255),
    customer_lastname VARCHAR(255),
    customer_gender VARCHAR(255),
    customer_email VARCHAR(255),
    customer_phone VARCHAR(255),
    signup_date VARCHAR(255),
    customer_age VARCHAR(255)
);


INSERT INTO sa_online_sales.src_online_sales
SELECT 
    store_id,
    store_name,
    store_website,
    supplier_id,
    supplier,
    supplier_email,
    contract_start,
    contract_end,
    product_id,
    product_name,
    brand,
    category,
    subcategory,
    rating,
    price,
    transaction_id,
    date,
    quantity_sold,
    amount_sold,
    cost_amount,
    payment_method,
    shipping_address,
    shipping_price,
    shipping_state,
    shipping_city,
	shipping_method,
    customer_id,
    customer_firstname,
    customer_lastname,
    customer_gender,
    customer_email,
    customer_phone,
    signup_date,
    customer_age
FROM sa_online_sales.ext_online_sales e
WHERE NOT EXISTS(
SELECT 1 FROM sa_online_sales.src_online_sales s 
WHERE e.transaction_id=s.transaction_id
AND e.date=s.date
AND e.product_id=s.product_id
AND e.customer_id=s.customer_id
AND e.store_id=s.store_id
AND e.supplier_id=s.supplier_id);
