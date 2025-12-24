CREATE SCHEMA IF NOT EXISTS BL_3NF;

CREATE SEQUENCE IF NOT EXISTS BL_3NF.seq_transaction_id START 1 INCREMENT BY 1;


CREATE TABLE IF NOT EXISTS BL_3NF.CE_Transactions (
    transaction_id BIGINT PRIMARY KEY,
    product_id BIGINT NOT NULL,
    employee_id BIGINT NOT NULL,
    shipping_id BIGINT NOT NULL,
    store_id BIGINT NOT NULL,
    customer_id BIGINT NOT NULL,
    payment_type_id BIGINT NOT NULL,
    supplier_id BIGINT NOT NULL,
    EVENT_DT TIMESTAMP NOT NULL,
    transaction_price DECIMAL NOT NULL,
    quantity_CNT INTEGER NOT NULL,
    revenue_TOT_AMT NUMERIC(12,2) NOT NULL,
    cost_AMT NUMERIC(12,3),
    TA_INSERT_DT TIMESTAMP NOT NULL,
	transaction_src_id VARCHAR(50) NOT NULL,
    source_system VARCHAR(50) NOT NULL,
    source_entity VARCHAR(50) NOT NULL,
    CONSTRAINT fk_transaction_shipping FOREIGN KEY (shipping_id) REFERENCES BL_3NF.CE_Shippings(shipping_id),
    CONSTRAINT fk_transaction_store FOREIGN KEY (store_id) REFERENCES BL_3NF.CE_Stores(store_id),
    CONSTRAINT fk_transaction_payment FOREIGN KEY (payment_type_id) REFERENCES BL_3NF.CE_PaymentTypes(payment_type_id),
    CONSTRAINT fk_transaction_supplier FOREIGN KEY (supplier_id) REFERENCES BL_3NF.CE_Suppliers(supplier_id)
);
