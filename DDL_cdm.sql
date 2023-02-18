--select * from information_schema.schemata s 
--drop table if exists cdm.user_product_counters;
--drop table if exists cdm.user_category_counters;

CREATE schema if not exists cdm;

create table if not exists cdm.user_product_counters
(
id integer PRIMARY KEY,
user_id UUID not null,
product_id uuid not null,
product_name varchar(255) not null,
order_cnt integer CHECK (order_cnt > 0) not null,
CONSTRAINT user_product_unique UNIQUE (user_id, product_id)
)

create table if not exists cdm.user_category_counters
(
id integer PRIMARY KEY,
user_id UUID not null,
category_id uuid not null,
category_name varchar(255) not null,
order_cnt integer CHECK (order_cnt > 0) not null
)