CREATE SCHEMA IF NOT EXISTS BL_DM;


CREATE TABLE IF NOT EXISTS BL_DM.DIM_Times (
    EVENT_TIME time PRIMARY KEY,
    hour_of_day smallint NOT NULL,
    minute_of_hour smallint NOT NULL,
    second_of_minute smallint NOT NULL,
    AM_PM varchar(10) NOT NULL,
    half_hour_flag boolean NOT NULL,
    quarter_hour smallint NOT NULL,
    day_part varchar(10) NOT NULL
);