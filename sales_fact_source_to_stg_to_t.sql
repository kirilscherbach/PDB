TRUNCATE TABLE staging.sales_fact_stg;
 
COPY staging.sales_fact_stg 
	   (client_order_id, client_id, supplier_order_id, item_id, supplier_id, order_source, 
       sales_manger, purchase_manager, legal_entity, warehouse, incoterms, 
       item_order_date, client_payment_terms, supplier_payment_terms, 
       advance_invoice_send_date, advance_invoice_paid_date, invoice_send_date, 
       invoice_paid_date, blank1, blank2, blank3, blank4, blank5, blank6, 
       item_dispatch_plan_date, blank7, blank8, item_dispatch_fact_date, 
       manufacturer, item_count, item_unit_of_measure, item_price, currency, 
       supplier_item_price, supplier_currency, blank9, blank10, blank11, 
       blank12)
FROM '/home/kscherbach/sales_fact.txt' WITH CSV HEADER DELIMITER '	' NULL 'NULL';

TRUNCATE TABLE staging.item_dmn_stg;

COPY staging.item_dmn_stg 
	   (item_id, item_addition_date, item_name, item_group, item_unit_of_measure)
FROM '/home/kscherbach/items.txt' WITH CSV HEADER DELIMITER '	' NULL 'NULL';

TRUNCATE TABLE staging.supplier_dmn_stg;

COPY staging.supplier_dmn_stg 
	   (supplier_id, supplier_addition_date, supplier_name, supplier_country, 
       supplier_industry)
FROM '/home/kscherbach/suppliers.txt' WITH CSV HEADER DELIMITER '	' NULL 'NULL';

TRUNCATE TABLE staging.customer_dmn_stg;

COPY staging.customer_dmn_stg 
	   (customer_id, customer_addition_date, customer_name, customer_country,
       customer_industry)
FROM '/home/kscherbach/clients.txt' WITH CSV HEADER DELIMITER '	' NULL 'NULL';


TRUNCATE TABLE public.sales_fact;
DROP INDEX IF EXISTS sales_fact_client_order_id_index;

INSERT INTO sales_fact 
(SELECT trim(both ' ' from client_order_id) as client_order_id, 
	trim(both ' ' from supplier_order_id) as supplier_order_id, 
	trim(both ' ' from item_id) as item_id, 
	trim(both ' ' from supplier_id) as supplier_id, 
	order_source, 
       sales_manger, purchase_manager, legal_entity, warehouse, incoterms, 
       to_date(item_order_date, 'DD.MM.YYYY') as item_order_date,
       client_payment_terms, supplier_payment_terms, 
       to_date(advance_invoice_send_date, 'DD.MM.YYYY') as advance_invoice_send_date, 
       to_date(advance_invoice_paid_date, 'DD.MM.YYYY') as advance_invoice_paid_date, 
       to_date(invoice_send_date, 'DD.MM.YYYY') as invoice_send_date, 
       to_date(invoice_paid_date, 'DD.MM.YYYY') as invoice_paid_date, 
       to_date(item_dispatch_plan_date, 'DD.MM.YYYY') as item_dispatch_plan_date, 
       to_date(item_dispatch_fact_date, 'DD.MM.YYYY') as item_dispatch_fact_date, 
       manufacturer, 
       to_number(item_count, '99999999999D9999999999999') as item_count, 
       item_unit_of_measure, 
       to_number(item_price, '99999999999D9999999999999') as item_price, 
       trim(both ' ' from currency) as currency, 
       to_number(supplier_item_price, '99999999999D9999999999999') as supplier_item_price, 
       trim(both ' ' from supplier_currency) as supplier_currency, 
       trim(both ' ' from client_id) as client_id
       FROM staging.sales_fact_stg);

CREATE INDEX sales_fact_client_order_id_index ON sales_fact(client_order_id);

TRUNCATE TABLE public.supplier_dmn;
DROP INDEX IF EXISTS supplier_index;

INSERT INTO public.supplier_dmn 
(SELECT supplier_id, to_date(supplier_addition_date, 'DD.MM.YYYY') as supplier_addition_date, supplier_name, supplier_country, 
       supplier_industry
  FROM staging.supplier_dmn_stg WHERE supplier_id<>''
);

CREATE UNIQUE INDEX supplier_index ON supplier_dmn (supplier_id);

TRUNCATE TABLE public.customer_dmn;
DROP INDEX IF EXISTS customer_index;

INSERT  INTO public.customer_dmn 
(SELECT customer_id, to_date(customer_addition_date, 'DD.MM.YYYY') as customer_addition_date, customer_name, customer_country, 
       customer_industry
  FROM staging.customer_dmn_stg WHERE customer_id<>''
);

CREATE UNIQUE INDEX customer_index ON customer_dmn (customer_id);

TRUNCATE TABLE public.item_dmn;
DROP INDEX IF EXISTS item_index;

INSERT INTO public.item_dmn
(SELECT DISTINCT item_id, to_date(item_addition_date, 'DD.MM.YYYY') as item_addition_date, item_name, item_unit_of_measure
  FROM staging.item_dmn_stg WHERE item_id<>''
);

CREATE UNIQUE INDEX item_index ON item_dmn (item_id);

/*DROP TABLE sales_item_pairing;

CREATE TABLE sales_item_pairing 
	(client_order_id text,
	initial_item_id text,
	item_order_date date,
	item_count numeric(24,13),
	item_price numeric(24,13),
	currency text,
	invoice_send_date date,
	order_lines_count numeric(24,13),
	order_items_count numeric(24,13),
	order_items_price numeric(24,13),
	complementary_item_id text); */

TRUNCATE TABLE sales_item_pairing;

INSERT INTO sales_item_pairing
	(SELECT 
	sales_fact_initial.client_order_id, 
	sales_fact_initial.item_id AS initial_item_id,
	sales_fact_initial.item_order_date,
	sales_fact_initial.item_count,
	sales_fact_initial.item_price,
	sales_fact_initial.currency,
	sales_fact_initial.invoice_send_date,
	COUNT(sales_fact_initial.item_id) OVER (PARTITION BY sales_fact_initial.client_order_id) as order_lines_count,
	SUM(sales_fact_initial.item_count) OVER (PARTITION BY sales_fact_initial.client_order_id) as order_items_count,
	SUM(sales_fact_initial.item_price) OVER (PARTITION BY sales_fact_initial.client_order_id) as order_items_price,
	sales_fact_complementary.item_id AS complementary_item_id
FROM 
	sales_fact AS sales_fact_initial
	INNER JOIN
	sales_fact AS sales_fact_complementary
		ON  sales_fact_initial.client_order_id=sales_fact_complementary.client_order_id
);

create table staging.xrte_daily ( xrte_date date,
  currency_code text,
  xrte_value numeric(24,13));

  ALTER TABLE xrte_daily
  OWNER TO afk_user;

COPY staging.xrte_daily 
	   (xrte_date, currency_code, xrte_value)
FROM '/home/kscherbach/usd2015.txt' WITH CSV HEADER DELIMITER '	' NULL 'NULL';

insert into public.xrte_daily (select xrte_date, currency_code, xrte_value from staging.xrte_daily);

insert into xrte_ranked (select rank() over (partition by currency_code order by xrte_date) as pk, xrte_date, currency_code, xrte_value from public.xrte_daily where xrte_date>'2014-01-01' and currency_code='BYR');


with base as 
(select 
x1.xrte_date
, x1.currency_code
, x1.xrte_value
, (x1.xrte_value+x2.xrte_value+x3.xrte_value)/3 as avg_xrte_value 
	from xrte_ranked x1 	
	inner join xrte_ranked x2 on x1.pk=x2.pk+1 and x1.currency_code=x2.currency_code 
	inner join xrte_ranked x3 on x1.pk=x3.pk-1 and x1.currency_code=x3.currency_code 
where x1.xrte_date between '2013-01-01' and '2015-12-31' and (DATE_PART('day',x1.xrte_date)=7 or DATE_PART('day',x1.xrte_date)=21)
)

select 	b1.xrte_date||' vs '||b2.xrte_date as caption
	, b1.xrte_value as payday_xrte
	, b2.xrte_value as advance_xrte
	, b1.xrte_value>b2.xrte_value -- check if xrte_rate on payday is higher then on advance day
	, b1.avg_xrte_value>b2.avg_xrte_value
	from 
	base b1 inner join base b2 
		on date_part('year', b1.xrte_date)=date_part('year', b2.xrte_date) 
		and date_part('month', b1.xrte_date)=date_part('month', b2.xrte_date)
		and date_part('day', b1.xrte_date)=7 and date_part('day', b2.xrte_date)=21;

