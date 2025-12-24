CREATE SCHEMA IF NOT EXISTS BL_3NF;

CREATE SEQUENCE IF NOT EXISTS BL_3NF.seq_customer_id START 1 INCREMENT BY 1;


CREATE TABLE IF NOT EXISTS BL_3NF.CE_Customers_SCD (
    customer_id BIGINT NOT NULL,
    customer_firstname VARCHAR(50) NOT NULL,
    customer_lastname VARCHAR(50) NOT NULL,
    customer_gender VARCHAR(20),
    customer_email VARCHAR(200),
    customer_phone VARCHAR(20),
    customer_age INTEGER,
	customer_signup_dt DATE NOT NULL,
    START_DT TIMESTAMP NOT NULL,
    END_DT TIMESTAMP NOT NULL,
    IS_ACTIVE VARCHAR(1) NOT NULL CHECK (IS_ACTIVE IN ('Y', 'N')),
    TA_INSERT_DT TIMESTAMP NOT NULL,
    customer_src_id VARCHAR(50) NOT NULL,
    source_system VARCHAR(50) NOT NULL,
    source_entity VARCHAR(50) NOT NULL,
    CONSTRAINT pk_ce_customers_scd PRIMARY KEY (customer_id, START_DT)
);


INSERT INTO BL_3NF.CE_Customers_SCD (
    customer_id, customer_firstname, customer_lastname, customer_gender, 
    customer_email, customer_phone, customer_age, customer_signup_dt, 
    START_DT, END_DT, IS_ACTIVE, TA_INSERT_DT, 
    customer_src_id, source_system, source_entity)
SELECT 
    -1, 'n. a.', 'n. a.', 'n. a.', 'n. a.', 'n. a.', -1, DATE '1900-01-01', 
    '1900-01-01'::timestamp, '9999-12-31'::timestamp, 'Y', '1900-01-01'::timestamp, 
    'n. a.', 'MANUAL', 'MANUAL'
WHERE NOT EXISTS (SELECT 1 FROM BL_3NF.CE_Customers_SCD WHERE customer_id = -1 AND START_DT = '1900-01-01'::timestamp);
