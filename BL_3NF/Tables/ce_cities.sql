CREATE SCHEMA IF NOT EXISTS BL_3NF;

CREATE SEQUENCE IF NOT EXISTS BL_3NF.seq_city_id START 1 INCREMENT BY 1;


CREATE TABLE IF NOT EXISTS BL_3NF.CE_Cities (
    city_id BIGINT PRIMARY KEY,
    city_name VARCHAR(100) NOT NULL,
    state_id BIGINT NOT NULL,
    TA_INSERT_DT TIMESTAMP NOT NULL,
    TA_UPDATE_DT TIMESTAMP NOT NULL,
    city_src_id VARCHAR(250) NOT NULL,
    source_system VARCHAR(50) NOT NULL,
    source_entity VARCHAR(50) NOT NULL,
    CONSTRAINT fk_city_state FOREIGN KEY (state_id) REFERENCES BL_3NF.CE_States(state_id),
	CONSTRAINT unq_cities_src UNIQUE (city_src_id, source_system, source_entity)
);


INSERT INTO BL_3NF.CE_Cities(city_id,city_name,state_id,TA_INSERT_DT,TA_UPDATE_DT,city_src_id,source_system,source_entity)
SELECT -1,'n. a.',-1,'1900-01-01'::timestamp,'1900-01-01'::timestamp,'n. a.','MANUAL','MANUAL'
WHERE NOT EXISTS(SELECT 1 FROM BL_3NF.CE_Cities WHERE city_id=-1);