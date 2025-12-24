CREATE SCHEMA IF NOT EXISTS BL_DM;

CREATE SEQUENCE IF NOT EXISTS BL_DM.seq_employee_surr_id START 1 INCREMENT BY 1;



CREATE TABLE IF NOT EXISTS BL_DM.DIM_Employees_SCD (
    Employee_SURR_ID bigint PRIMARY KEY,
    Employee_Firstname varchar(50) NOT NULL,
    Employee_Lastname varchar(50) NOT NULL,
    Employee_Gender varchar(6),
    Employee_Email varchar(100),
    Employee_Hire_DT date NOT NULL,
    Start_DT timestamp NOT NULL,
    End_DT timestamp NOT NULL,
    Is_Active VARCHAR(1) NOT NULL,
    TA_Insert_DT timestamp NOT NULL,
	Employee_SRC_ID varchar(250) NOT NULL,
    source_system varchar(100) NOT NULL,
    source_entity varchar(100) NOT NULL
);



INSERT INTO BL_DM.DIM_Employees_SCD(
Employee_SURR_ID,Employee_SRC_ID,Employee_Firstname,Employee_Lastname,Employee_Gender,Employee_Email,Employee_Hire_DT,
Start_DT,End_DT,Is_Active,TA_Insert_DT,source_system,source_entity)
SELECT -1,'n. a.','n. a.','n. a.','n. a.','n. a.','1900-01-01'::timestamp,'1900-01-01'::timestamp,'9999-12-31'::timestamp,'Y','1900-01-01'::timestamp,'MANUAL','MANUAL'
WHERE NOT EXISTS (SELECT 1 FROM BL_DM.DIM_Employees_SCD WHERE Employee_SURR_ID=-1);
