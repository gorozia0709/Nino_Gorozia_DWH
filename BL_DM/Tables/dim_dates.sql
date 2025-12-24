CREATE SCHEMA IF NOT EXISTS BL_DM;

CREATE TABLE IF NOT EXISTS BL_DM.DIM_Dates (
    EVENT_DT date PRIMARY KEY,
    calendar_year int NOT NULL,
    Month_NUM smallint NOT NULL,
    Month_Name varchar(9) NOT NULL,
    Quarter_NUM smallint NOT NULL,
    Week_NUM smallint NOT NULL,
    Day_NUM smallint NOT NULL,
    Day_Name varchar(10) NOT NULL,
    is_weekend boolean NOT NULL
);

