CREATE SCHEMA IF NOT EXISTS BL_3NF;

CREATE SEQUENCE IF NOT EXISTS BL_3NF.seq_brand_id START 1 INCREMENT BY 1;


CREATE TABLE IF NOT EXISTS BL_3NF.CE_Brands (
    brand_id BIGINT PRIMARY KEY,
    brand_name VARCHAR(100) NOT NULL,
    TA_INSERT_DT TIMESTAMP NOT NULL,
    TA_UPDATE_DT TIMESTAMP NOT NULL,
    brand_src_id VARCHAR(250) NOT NULL,
    source_system VARCHAR(50) NOT NULL,
    source_entity VARCHAR(50) NOT NULL,
	CONSTRAINT unq_brands_src UNIQUE (brand_src_id, source_system, source_entity)
);


INSERT INTO BL_3NF.CE_Brands(brand_id,brand_name,TA_INSERT_DT,TA_UPDATE_DT,brand_src_id,source_system,source_entity)
SELECT -1,'n. a.','1900-01-01'::timestamp,'1900-01-01'::timestamp,'n. a.','MANUAL','MANUAL'
WHERE NOT EXISTS(SELECT 1 FROM BL_3NF.CE_Brands WHERE brand_id=-1);