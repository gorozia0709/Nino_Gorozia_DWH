CREATE SCHEMA IF NOT EXISTS BL_3NF;

CREATE SEQUENCE IF NOT EXISTS BL_3NF.seq_product_id START 1 INCREMENT BY 1;


CREATE TABLE IF NOT EXISTS BL_3NF.CE_Products_SCD (
    product_id BIGINT NOT NULL,
    product_name VARCHAR(100) NOT NULL,
    subcategory_id BIGINT NOT NULL,
    brand_id BIGINT NOT NULL,
	product_rating DECIMAL(3,2) NOT NULL,
    START_DT TIMESTAMP NOT NULL,
    END_DT TIMESTAMP NOT NULL,
    IS_ACTIVE VARCHAR(1) NOT NULL,
    TA_INSERT_DT TIMESTAMP NOT NULL,
	product_src_id VARCHAR(250) NOT NULL,
    source_system VARCHAR(50) NOT NULL,
    source_entity VARCHAR(50) NOT NULL,
    CONSTRAINT pk_ce_products_scd PRIMARY KEY (product_id, START_DT),
    CONSTRAINT fk_product_subcategory FOREIGN KEY (subcategory_id) REFERENCES BL_3NF.CE_Subcategories(subcategory_id),
    CONSTRAINT fk_product_brand FOREIGN KEY (brand_id) REFERENCES BL_3NF.CE_Brands(brand_id)
);


INSERT INTO BL_3NF.CE_Products_SCD(product_id,product_name,product_rating,subcategory_id,brand_id,START_DT,END_DT,IS_ACTIVE,TA_INSERT_DT,product_src_id,source_system,source_entity)
SELECT -1,'n. a.',-1,-1,-1,'1900-01-01'::timestamp, '9999-12-31'::timestamp,'Y', '1900-01-01'::timestamp,'n. a.','MANUAL','MANUAL'
WHERE NOT EXISTS(SELECT 1 FROM BL_3NF.CE_Products_SCD WHERE product_id=-1);