TRUNCATE TABLE date_dmn;
DROP INDEX IF EXISTS date_dmn_index;

INSERT INTO date_dmn (full_date, 
		full_date_description,
		day_of_week,
		month_text,
		month_number,
		"year",
		weekday_indicator) 
SELECT
	day,
	rtrim(to_char(day, 'Month')) || to_char(day, ' DD, YYYY'),
	to_char(day, 'Day'),
	rtrim(to_char(day, 'Month')),
	date_part('month', day),
	date_part('year', day),
    CASE
        WHEN date_part('isodow', day) IN (6, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END
FROM
    generate_series('2010-01-01'::date, '2020-12-31'::date, '1 day') day;

CREATE UNIQUE INDEX date_dmn_index ON date_dmn(full_date);