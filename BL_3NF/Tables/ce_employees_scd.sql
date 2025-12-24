CREATE SCHEMA IF NOT EXISTS BL_3NF;

CREATE SEQUENCE IF NOT EXISTS BL_3NF.seq_employee_id START 1 INCREMENT BY 1;


CREATE TABLE IF NOT EXISTS BL_3NF.CE_Employees_SCD (
    employee_id BIGINT NOT NULL,
    employee_firstname VARCHAR(50) NOT NULL,
    employee_lastname VARCHAR(50) NOT NULL,
    employee_gender VARCHAR(6),
    employee_email VARCHAR(200),
    store_id BIGINT NOT NULL,
	employee_hire_dt DATE NOT NULL, 
    START_DT TIMESTAMP NOT NULL,
    END_DT TIMESTAMP NOT NULL,
    IS_ACTIVE VARCHAR(1) NOT NULL CHECK (IS_ACTIVE IN ('Y', 'N')),
    TA_INSERT_DT TIMESTAMP NOT NULL,
    employee_src_id VARCHAR(250) NOT NULL,
    source_system VARCHAR(50) NOT NULL,
    source_entity VARCHAR(50) NOT NULL,
    CONSTRAINT pk_ce_employees_scd PRIMARY KEY (employee_id, START_DT),
    CONSTRAINT fk_employee_store FOREIGN KEY (store_id) REFERENCES BL_3NF.CE_Stores(store_id)
);


INSERT INTO BL_3NF.CE_Employees_SCD ( employee_id, employee_firstname, employee_lastname, employee_gender, employee_email, 
employee_hire_dt, store_id, start_dt, end_dt, is_active, ta_insert_dt, employee_src_id, source_system, source_entity ) 
SELECT -1, 'n. a.', 'n. a.', 'n. a.', 'n. a.', DATE '1900-01-01', -1,  '1900-01-01'::TIMESTAMP,  '9999-12-31'::TIMESTAMP, 'Y', 
'1900-01-01 '::TIMESTAMP, 'n. a.', 'MANUAL', 'MANUAL' 
WHERE NOT EXISTS ( SELECT 1 FROM BL_3NF.CE_Employees_SCD WHERE employee_id = -1 AND start_dt = '1900-01-01'::TIMESTAMP );