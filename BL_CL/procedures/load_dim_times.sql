CREATE SCHEMA IF NOT EXISTS BL_CL;


CREATE OR REPLACE PROCEDURE bl_cl.populate_dim_times()
LANGUAGE plpgsql
AS $$
DECLARE
t TIME := TIME '00:00:00';
s INT := 0;
day_part_label VARCHAR(20);
BEGIN
WHILE s < 86400 LOOP
day_part_label := CASE
WHEN EXTRACT(HOUR FROM t) BETWEEN 0 AND 5 THEN 'Night'
WHEN EXTRACT(HOUR FROM t) BETWEEN 6 AND 11 THEN 'Morning'
WHEN EXTRACT(HOUR FROM t) BETWEEN 12 AND 17 THEN 'Afternoon'
ELSE 'Evening' END;

INSERT INTO BL_DM.DIM_TIMES(event_time,hour_of_day,minute_of_hour,second_of_minute,am_pm,half_hour_flag,quarter_hour,day_part)
VALUES(
t,
EXTRACT(HOUR FROM t)::SMALLINT,
EXTRACT(MINUTE FROM t)::SMALLINT,
EXTRACT(SECOND FROM t)::SMALLINT,
CASE WHEN EXTRACT(HOUR FROM t) < 12 THEN 'AM' ELSE 'PM' END,
EXTRACT(MINUTE FROM t)::SMALLINT IN (0,30),
FLOOR(EXTRACT(MINUTE FROM t)/15)::SMALLINT * 15,
day_part_label)
ON CONFLICT(event_time) DO NOTHING;

s := s + 1;
t := TIME '00:00:00' + (s || ' seconds')::INTERVAL;
END LOOP;
END;
$$;