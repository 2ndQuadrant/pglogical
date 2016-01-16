--FOREIGN KEY
SELECT * FROM pglogical_regress_variables()
\gset


\c :provider_dsn

SELECT pglogical.replicate_ddl_command($$
CREATE TABLE public.f1k_products (
    product_no integer PRIMARY KEY,
    product_id integer,
    name text,
    price numeric
);

CREATE TABLE public.f1k_orders (
    order_id integer,
    product_no integer REFERENCES public.f1k_products (product_no),
    quantity integer
);
--pass
$$);

SELECT * FROM pglogical.replication_set_add_table('default', 'f1k_products');
SELECT * FROM pglogical.replication_set_add_table('default_insert_only', 'f1k_orders');

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;

INSERT into public.f1k_products VALUES (1, 1, 'product1', 1.20);
INSERT into public.f1k_products VALUES (2, 2, 'product2', 2.40);

INSERT into public.f1k_orders VALUES (300, 1, 4);
INSERT into public.f1k_orders VALUES (22, 2, 14);
INSERT into public.f1k_orders VALUES (23, 2, 24);
INSERT into public.f1k_orders VALUES (24, 2, 40);

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;

\c :subscriber_dsn

SELECT * FROM public.f1k_products;
SELECT * FROM public.f1k_orders;


\c :provider_dsn
\set VERBOSITY terse
SELECT pglogical.replicate_ddl_command($$
	DROP TABLE public.f1k_orders CASCADE;
	DROP TABLE public.f1k_products CASCADE;
$$);
