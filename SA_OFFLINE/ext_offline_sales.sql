CREATE SCHEMA IF NOT EXISTS sa_offline_sales;

CREATE EXTENSION IF NOT EXISTS file_fdw;
CREATE SERVER IF NOT EXISTS csv_server FOREIGN DATA WRAPPER file_fdw;


CREATE FOREIGN TABLE sa_offline_sales.ext_offline_sales (
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
)
SERVER csv_server
OPTIONS (
    filename 'C:\\Program Files\\PostgreSQL\\16\\data\\offline_5_percent.csv',	
    format 'csv',
    header 'true',
    delimiter ','
);
