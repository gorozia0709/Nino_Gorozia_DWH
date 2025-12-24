CREATE SCHEMA IF NOT EXISTS BL_3NF;

CREATE SEQUENCE IF NOT EXISTS BL_3NF.seq_shipping_id START 1 INCREMENT BY 1;


CREATE TABLE IF NOT EXISTS BL_3NF.CE_Shippings (
    shipping_id BIGINT PRIMARY KEY,
    shipping_method VARCHAR(40) NOT NULL,
    shipping_price DECIMAL NOT NULL,
    address_id BIGINT NOT NULL,
    TA_INSERT_DT TIMESTAMP NOT NULL,
    TA_UPDATE_DT TIMESTAMP NOT NULL,
    shipping_src_id VARCHAR(250) NOT NULL,
    source_system VARCHAR(50) NOT NULL,
    source_entity VARCHAR(50) NOT NULL,
    CONSTRAINT fk_shipping_address FOREIGN KEY (address_id) REFERENCES BL_3NF.CE_Addresses(address_id),
	CONSTRAINT unq_shippings_src UNIQUE (shipping_src_id, source_system, source_entity)
);




INSERT INTO BL_3NF.CE_Shippings(shipping_id,shipping_method,shipping_price,address_id,TA_INSERT_DT,TA_UPDATE_DT,shipping_src_id,source_system,source_entity)
SELECT -1,'n. a.',-1,-1,'1900-01-01'::timestamp,'1900-01-01'::timestamp,'n. a.','MANUAL','MANUAL'
WHERE NOT EXISTS(SELECT 1 FROM BL_3NF.CE_Shippings WHERE shipping_id=-1);