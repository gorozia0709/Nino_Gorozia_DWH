CREATE SCHEMA IF NOT EXISTS BL_DM;

CREATE SEQUENCE IF NOT EXISTS BL_DM.seq_store_surr_id START 1 INCREMENT BY 1;


CREATE TABLE IF NOT EXISTS BL_DM.DIM_Stores (
    Store_SURR_ID bigint PRIMARY KEY,
    Store_Name varchar(100) NOT NULL,
    Store_Type varchar(50) NOT NULL,
    Store_Website varchar(100),
    Store_Address_ID bigint NOT NULL,
    Store_Address_Name varchar(100) NOT NULL,
    Store_City_ID bigint NOT NULL,
    Store_City_Name varchar(50) NOT NULL,
    Store_State_ID bigint NOT NULL,
    Store_State_Name varchar(10) NOT NULL,
    TA_Insert_DT timestamp NOT NULL,
    TA_Update_DT timestamp NOT NULL,
	store_src_id varchar(200) NOT NULL,
    source_system varchar(100) NOT NULL,
    source_entity varchar(100) NOT NULL
);




INSERT INTO BL_DM.DIM_Stores ( Store_SURR_ID, Store_SRC_ID, Store_Name, Store_Type, Store_Website, Store_Address_ID, Store_Address_Name, 
Store_City_ID, Store_City_Name, Store_State_ID, Store_State_Name, TA_Insert_DT, TA_Update_DT, source_system, source_entity ) 
VALUES ( -1, 'n. a.', 'n. a.', 'n. a.', 'n. a.', -1, 'n. a.', -1, 'n. a.', -1, 'n. a.', '1900-01-01'::timestamp, '1900-01-01'::timestamp, 'MANUAL', 'MANUAL' ) 
ON CONFLICT (store_surr_id) DO NOTHING;