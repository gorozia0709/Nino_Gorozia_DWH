CREATE SCHEMA IF NOT EXISTS BL_3NF;

CREATE SEQUENCE IF NOT EXISTS BL_3NF.seq_address_id START 1 INCREMENT BY 1;


CREATE TABLE IF NOT EXISTS BL_3NF.CE_Addresses (
    address_id BIGINT PRIMARY KEY,
    address_name VARCHAR(200) NOT NULL,
    city_id BIGINT NOT NULL,
    TA_INSERT_DT TIMESTAMP NOT NULL,
    TA_UPDATE_DT TIMESTAMP NOT NULL,
    address_src_id VARCHAR(250) NOT NULL,
    source_system VARCHAR(50) NOT NULL,
    source_entity VARCHAR(50) NOT NULL,
    CONSTRAINT fk_address_city FOREIGN KEY (city_id) REFERENCES BL_3NF.CE_Cities(city_id),
	CONSTRAINT unq_addresses_src UNIQUE (address_src_id, source_system, source_entity)
);



INSERT INTO BL_3NF.CE_Addresses(address_id,address_name,city_id,TA_INSERT_DT,TA_UPDATE_DT,address_src_id,source_system,source_entity)
SELECT -1,'n. a.',-1,'1900-01-01'::timestamp,'1900-01-01'::timestamp,'n. a.','MANUAL','MANUAL'
WHERE NOT EXISTS(SELECT 1 FROM BL_3NF.CE_Addresses WHERE address_id=-1);