--FOREIGN KEY

\c regression

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

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;

INSERT into public.f1k_products VALUES (1, 1, 'product1', 1.20);
INSERT into public.f1k_products VALUES (2, 2, 'product2', 2.40);

INSERT into public.f1k_orders VALUES (300, 1, 4);
INSERT into public.f1k_orders VALUES (22, 2, 14);
INSERT into public.f1k_orders VALUES (23, 2, 24);
INSERT into public.f1k_orders VALUES (24, 2, 40);

SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), pid) FROM pg_stat_replication;

\c postgres

SELECT * FROM public.f1k_products;
SELECT * FROM public.f1k_orders;


\c regression

SELECT pglogical.replicate_ddl_command($$
DROP TABLE public.f1k_orders;
DROP TABLE public.f1k_products;
$$);
