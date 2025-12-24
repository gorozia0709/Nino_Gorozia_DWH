CREATE SCHEMA IF NOT EXISTS BL_CL;




CREATE OR REPLACE PROCEDURE bl_cl.populate_dim_dates(p_start_date DATE, p_end_date DATE)
LANGUAGE plpgsql
AS $$
DECLARE
d DATE := p_start_date;
BEGIN
IF p_end_date<p_start_date THEN
RAISE EXCEPTION 'end date (%) must be >= start date (%)', p_end_date, p_start_date;
END IF;

WHILE d <= p_end_date LOOP
INSERT INTO BL_DM.DIM_DATES (
event_dt,
calendar_year,
month_num,
month_name,
quarter_num,
week_num,
day_num,
day_name,
is_weekend
)
VALUES (
d,
EXTRACT(YEAR  FROM d)::INT,
EXTRACT(MONTH FROM d)::SMALLINT,
TO_CHAR(d,'FMMonth'),
EXTRACT(QUARTER FROM d)::SMALLINT,
EXTRACT(WEEK FROM d)::SMALLINT,
EXTRACT(DAY FROM d)::SMALLINT,
TO_CHAR(d,'FMDay'),
(EXTRACT(DOW FROM d) IN (0,6))
)

ON CONFLICT (event_dt) DO NOTHING;
d := d + INTERVAL '1 day';
END LOOP;
END;
$$;
