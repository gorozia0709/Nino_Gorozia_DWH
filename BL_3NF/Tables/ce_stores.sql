CREATE SCHEMA IF NOT EXISTS BL_3NF;

CREATE SEQUENCE IF NOT EXISTS BL_3NF.seq_store_id START 1 INCREMENT BY 1;

CREATE TABLE IF NOT EXISTS BL_3NF.CE_Stores (
    store_id BIGINT PRIMARY KEY,
    store_name VARCHAR(100) NOT NULL,
    store_type VARCHAR(50) NOT NULL,
    store_website VARCHAR(200),
    address_id BIGINT NOT NULL,
    TA_INSERT_DT TIMESTAMP NOT NULL,
    TA_UPDATE_DT TIMESTAMP NOT NULL,
    store_src_id VARCHAR(250) NOT NULL,
    source_system VARCHAR(50) NOT NULL,
    source_entity VARCHAR(50) NOT NULL,
    CONSTRAINT fk_store_address FOREIGN KEY (address_id) REFERENCES BL_3NF.CE_Addresses(address_id),
	CONSTRAINT unq_stores_src UNIQUE (store_src_id, source_system, source_entity)
);



INSERT INTO BL_3NF.CE_Stores(store_id,store_name,store_type,store_website,address_id,TA_INSERT_DT,TA_UPDATE_DT,store_src_id,source_system,source_entity)
SELECT -1,'n. a.','n. a.','n. a.',-1,'1900-01-01'::timestamp,'1900-01-01'::timestamp,'n. a.','MANUAL','MANUAL'
WHERE NOT EXISTS(SELECT 1 FROM BL_3NF.CE_Stores WHERE store_id=-1);