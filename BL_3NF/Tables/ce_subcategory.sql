CREATE SCHEMA IF NOT EXISTS BL_3NF;

CREATE SEQUENCE IF NOT EXISTS BL_3NF.seq_subcategory_id START 1 INCREMENT BY 1;

CREATE TABLE IF NOT EXISTS BL_3NF.CE_Subcategories (
    subcategory_id BIGINT PRIMARY KEY,
    subcategory_name VARCHAR(100) NOT NULL,
    category_id BIGINT NOT NULL,
    TA_INSERT_DT TIMESTAMP NOT NULL,
    TA_UPDATE_DT TIMESTAMP NOT NULL,
    subcategory_src_id VARCHAR(250) NOT NULL,
    source_system VARCHAR(50) NOT NULL,
    source_entity VARCHAR(50) NOT NULL,
    CONSTRAINT fk_subcategory_category FOREIGN KEY (category_id) REFERENCES BL_3NF.CE_Categories(category_id),
	CONSTRAINT unq_subcategories_src UNIQUE (subcategory_src_id, source_system, source_entity)
);



INSERT INTO BL_3NF.CE_Subcategories(subcategory_id,subcategory_name,category_id,TA_INSERT_DT,TA_UPDATE_DT,subcategory_src_id,source_system,source_entity)
SELECT -1,'n. a.',-1,'1900-01-01'::timestamp,'1900-01-01'::timestamp,'n. a.','MANUAL','MANUAL'
WHERE NOT EXISTS(SELECT 1 FROM BL_3NF.CE_Subcategories WHERE subcategory_id=-1);