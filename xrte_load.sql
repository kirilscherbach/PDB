create table staging.xrte_daily ( xrte_date date,
  xrte_code text,
  xrte_value numeric(24,13));

  ALTER TABLE xrte_daily
  OWNER TO afk_user;

COPY staging.xrte_daily 
	   (xrte_date, xrte_code, xrte_value)
FROM '/home/kscherbach/usd2015.txt' WITH CSV HEADER DELIMITER '	' NULL 'NULL';

insert into public.xrte_daily 
	(select a.xrte_date, a.currency_code, a.xrte_value 
		from staging.xrte_daily a left outer join public.xrte_daily b 
		on a.xrte_date=b.xrte_date and a.currency_code=b.currency_code 
		where b.xrte_date is null);

insert into xrte_ranked (select rank() over (partition by xrte_code order by xrte_date) as pk, xrte_date, xrte_code, xrte_value from public.xrte_daily where xrte_date>'2014-01-01' and xrte_code='BYR');


WITH base AS
  (SELECT x1.xrte_date ,
          x1.xrte_code ,
          x1.xrte_value ,
          (x1.xrte_value+x2.xrte_value+x3.xrte_value)/3 AS avg_xrte_value
   FROM xrte_ranked x1
   INNER JOIN xrte_ranked x2 ON x1.pk=x2.pk+1
   AND x1.xrte_code=x2.xrte_code
   INNER JOIN xrte_ranked x3 ON x1.pk=x3.pk-1
   AND x1.xrte_code=x3.xrte_code
   WHERE x1.xrte_date BETWEEN '2013-01-01' AND '2015-12-31'
     AND (DATE_PART('day',x1.xrte_date)=7
          OR DATE_PART('day',x1.xrte_date)=21)),
     ym_xrte AS
  (SELECT date_part('month', CASE WHEN date_part('day', x1.xrte_date)<7
                    AND date_part('month', x1.xrte_date)>1 THEN to_date(date_part('day', x1.xrte_date)||'.'||(date_part('month', x1.xrte_date)-1)||'.'||date_part('year', x1.xrte_date), 'DD.MM.YYYY') WHEN date_part('day', x1.xrte_date)<7
                    AND date_part('month', x1.xrte_date)=1 THEN to_date(date_part('day', x1.xrte_date)||'.'||12||'.'||(date_part('year', x1.xrte_date)-1), 'DD.MM.YYYY') ELSE x1.xrte_date END) AS xrte_month,
          date_part('year', CASE WHEN date_part('day', x1.xrte_date)<7
                    AND date_part('month', x1.xrte_date)>1 THEN to_date(date_part('day', x1.xrte_date)||'.'||(date_part('month', x1.xrte_date)-1)||'.'||date_part('year', x1.xrte_date), 'DD.MM.YYYY') WHEN date_part('day', x1.xrte_date)<7
                    AND date_part('month', x1.xrte_date)=1 THEN to_date(date_part('day', x1.xrte_date)||'.'||12||'.'||(date_part('year', x1.xrte_date)-1), 'DD.MM.YYYY') ELSE x1.xrte_date END) AS xrte_year,
          x1.xrte_code ,
          x1.xrte_value
   FROM xrte_ranked x1),
     ma_xrte AS
  (SELECT DISTINCT xrte_year,
                   xrte_month,
                   min(xrte_value) over (partition BY xrte_year, xrte_month) AS min_xrte_value,
                                   avg(xrte_value) over (partition BY xrte_year, xrte_month) AS avg_xrte_value
   FROM ym_xrte)


SELECT b1.xrte_date||' vs '||b2.xrte_date AS caption ,
                             round(b1.xrte_value) AS payday_xrte ,
                             round(b2.xrte_value) AS advance_xrte ,
                             round(b3.min_xrte_value) as minimal_value,
                             round(b3.avg_xrte_value) as avg_xrte_value
FROM base b1
INNER JOIN base b2 ON date_part('year', b1.xrte_date)=date_part('year', b2.xrte_date)
AND date_part('month', b1.xrte_date)=date_part('month', b2.xrte_date)
AND date_part('day', b1.xrte_date)=7
AND date_part('day', b2.xrte_date)=21
INNER JOIN ma_xrte b3 on date_part('year', b1.xrte_date)=b3.xrte_year
AND date_part('month', b1.xrte_date)=b3.xrte_month
order by 1;

select x1.xrte_date, x1.xrte_value, b3.min_xrte_value, b3.avg_xrte_value, x2.xrte_value as payday_xrte from ym_xrte x1 
inner join ma_xrte b3 on x1.xrte_year=b3.xrte_year AND x1.xrte_month=b3.xrte_month
inner join ym_xrte x2 on x1.xrte_year=x2.xrte_year AND x1.xrte_month=x2.xrte_month and x2.xrte_day=7
