TRUNCATE TABLE sales_fact;

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
