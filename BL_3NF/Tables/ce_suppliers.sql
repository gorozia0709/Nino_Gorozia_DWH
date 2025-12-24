CREATE SCHEMA IF NOT EXISTS BL_3NF;

CREATE SEQUENCE IF NOT EXISTS BL_3NF.seq_supplier_id START 1 INCREMENT BY 1;



CREATE TABLE IF NOT EXISTS BL_3NF.CE_Suppliers (
    supplier_id BIGINT PRIMARY KEY,
    supplier_name VARCHAR(100) NOT NULL,
    supplier_email VARCHAR(200),
    supplier_contract_start_dt DATE NOT NULL,
    supplier_contract_end_dt DATE NOT NULL,
    TA_INSERT_DT TIMESTAMP NOT NULL,
    TA_UPDATE_DT TIMESTAMP NOT NULL,
    supplier_src_id VARCHAR(250) NOT NULL,
    source_system VARCHAR(50) NOT NULL,
    source_entity VARCHAR(50) NOT NULL,
	CONSTRAINT unq_suppliers_src UNIQUE (supplier_src_id, source_system, source_entity)
);


INSERT INTO BL_3NF.CE_Suppliers( supplier_id, supplier_name, supplier_email, supplier_contract_start_dt, 
supplier_contract_end_dt, TA_INSERT_DT, TA_UPDATE_DT, supplier_src_id, source_system, source_entity ) 
SELECT -1, 'n. a.', 'n. a.', '1900-01-01'::date, '9999-12-31'::date, '1900-01-01'::timestamp, '9999-12-31'::timestamp, 'n. a.', 'MANUAL', 'MANUAL' 
WHERE NOT EXISTS ( SELECT 1 FROM BL_3NF.CE_Suppliers WHERE supplier_id = -1 );