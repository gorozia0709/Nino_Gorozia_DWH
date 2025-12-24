CREATE SCHEMA IF NOT EXISTS BL_DM;
CREATE SEQUENCE IF NOT EXISTS BL_DM.seq_supplier_surr_id START 1 INCREMENT BY 1;



CREATE TABLE IF NOT EXISTS BL_DM.DIM_Suppliers (
    Supplier_SURR_ID bigint PRIMARY KEY,
    Supplier_Name varchar(50) NOT NULL,
    Supplier_Email varchar(100),
    Supplier_Contract_Start_DT date NOT NULL,
    Supplier_Contract_End_DT date NOT NULL,
    TA_Insert_DT timestamp NOT NULL,
    TA_Update_DT timestamp NOT NULL,
	Supplier_SRC_ID varchar(200) NOT NULL,
    source_system varchar(100) NOT NULL,
    source_entity varchar(100) NOT NULL
);


INSERT INTO BL_DM.DIM_Suppliers ( Supplier_SURR_ID, Supplier_SRC_ID, Supplier_Name, Supplier_Email, Supplier_Contract_Start_DT, Supplier_Contract_End_DT, TA_Insert_DT, TA_Update_DT, source_system, source_entity ) 
VALUES ( -1, 'n. a.', 'n. a.', 'n. a.', DATE '1900-01-01', DATE '1900-01-01', '1900-01-01'::timestamp, '1900-01-01'::timestamp, 'MANUAL', 'MANUAL' ) 
ON CONFLICT (Supplier_SURR_ID) DO NOTHING;