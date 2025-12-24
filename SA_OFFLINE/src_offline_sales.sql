CREATE TABLE IF NOT EXISTS sa_offline_sales.src_offline_sales (
    store_id VARCHAR(255),
    store_name VARCHAR(255),
    store_type VARCHAR(255),
    store_state VARCHAR(255),
    store_city VARCHAR(255),
    store_address VARCHAR(255),
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
    employee_id VARCHAR(255),
    employee_firstname VARCHAR(255),
    employee_lastname VARCHAR(255),
    gender VARCHAR(255),
    employee_email VARCHAR(255),
    hire_date VARCHAR(255)
);

INSERT INTO sa_offline_sales.src_offline_sales
SELECT 
    store_id,
    store_name,
    store_type,
    store_state,
    store_city,
    store_address,
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
    employee_id,
    employee_firstname,
    employee_lastname,
    gender,
    employee_email,
    hire_date
FROM sa_offline_sales.ext_offline_sales e
WHERE NOT EXISTS(
SELECT 1 FROM sa_offline_sales.src_offline_sales s 
WHERE e.transaction_id=s.transaction_id
AND e.date=s.date
AND e.product_id=s.product_id
AND e.employee_id=s.employee_id
AND e.store_id=s.store_id
AND e.supplier_id=s.supplier_id);
