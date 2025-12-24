CREATE SCHEMA IF NOT EXISTS BL_3NF;

CREATE SEQUENCE IF NOT EXISTS BL_3NF.seq_state_id START 1 INCREMENT BY 1;



CREATE TABLE IF NOT EXISTS BL_3NF.CE_States (
    state_id BIGINT PRIMARY KEY,
    state_name VARCHAR(100) NOT NULL,
    TA_INSERT_DT TIMESTAMP NOT NULL,
    TA_UPDATE_DT TIMESTAMP NOT NULL,
    state_src_id VARCHAR(250) NOT NULL,
    source_system VARCHAR(50) NOT NULL,
    source_entity VARCHAR(50) NOT NULL,
	CONSTRAINT unq_states_src UNIQUE (state_src_id, source_system, source_entity)
);


INSERT INTO BL_3NF.CE_States(state_id, state_name, TA_INSERT_DT, TA_UPDATE_DT, state_src_id, source_system, source_entity)
SELECT -1, 'n. a.', '1900-01-01'::timestamp, '1900-01-01'::timestamp, 'n. a.', 'MANUAL', 'MANUAL'
WHERE NOT EXISTS (SELECT 1 FROM BL_3NF.CE_States WHERE state_id = -1);