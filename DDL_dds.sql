--CREATE EXTENSION "uuid-ossp";

--- HUB-tables ---
--drop table if exists dds.h_user CASCADE;
--drop table if exists dds.h_product CASCADE;
--drop table if exists dds.h_category CASCADE;
--drop table if exists dds.h_restaurant CASCADE;
--drop table if exists dds.h_order CASCADE;


create table if not exists dds.h_user
(
h_user_pk VARCHAR  PRIMARY KEY, --UUID
user_id VARCHAR  not null,
load_dt timestamp not null,
load_src varchar not null
);

create table if not exists dds.h_product
(
h_product_pk VARCHAR PRIMARY KEY, -- UUID
product_id VARCHAR  not null,
load_dt timestamp not null,
load_src varchar not null
);
create table if not exists dds.h_category
(
h_category_pk VARCHAR PRIMARY KEY, -- UUID
category_name VARCHAR  not null,
load_dt timestamp not null,
load_src varchar not null
);

create table if not exists dds.h_restaurant
(
h_restaurant_pk VARCHAR PRIMARY KEY, -- UUID
restaurant_id VARCHAR  not null,
load_dt timestamp not null,
load_src varchar not null
);
create table if not exists dds.h_order
(
h_order_pk VARCHAR PRIMARY KEY, -- UUID
order_id integer  not null,
order_dt timestamp  not null,
load_dt timestamp not null,
load_src varchar not null
);


-- Satelit-tables --

--drop table if exists dds.s_user_names;
--drop table if exists dds.s_product_names;
--drop table if exists dds.s_restaurant_names;
--drop table if exists dds.s_order_cost;
--drop table if exists dds.s_order_status CASCADE;

create table if not exists dds.s_user_names
(
hk_user_names_pk VARCHAR PRIMARY KEY, -- UUID
h_user_pk VARCHAR not null, -- UUID
username varchar not null,
userlogin varchar not null,
load_dt timestamp not null,
load_src varchar not null,
CONSTRAINT fk_h_user
   FOREIGN KEY(h_user_pk) 
      REFERENCES dds.h_user(h_user_pk));

create table if not exists dds.s_product_names
(
hk_product_names_pk VARCHAR PRIMARY KEY, -- UUID
h_product_pk VARCHAR, -- UUID
name varchar not null,
load_dt timestamp not null,
load_src varchar not null,
CONSTRAINT fk_h_product
   FOREIGN KEY(h_product_pk) 
      REFERENCES dds.h_product(h_product_pk));

create table if not exists dds.s_restaurant_names
(
hk_restaurant_names_pk VARCHAR PRIMARY KEY, -- UUID
h_restaurant_pk VARCHAR not null, -- UUID
name varchar not null,
load_dt timestamp not null,
load_src varchar not null,
CONSTRAINT fk_h_restaurant
   FOREIGN KEY(h_restaurant_pk) 
      REFERENCES dds.h_restaurant(h_restaurant_pk));

create table if not exists dds.s_order_cost
(
hk_order_cost_pk VARCHAR PRIMARY KEY, -- UUID
h_order_pk VARCHAR not null, -- UUID
cost decimal(18,5) not null,
payment decimal(18,5) not null,
load_dt timestamp not null,
load_src varchar not null,
CONSTRAINT fk_order
   FOREIGN KEY(h_order_pk) 
      REFERENCES dds.h_order(h_order_pk));

create table if not exists dds.s_order_status
(
hk_order_status_pk VARCHAR PRIMARY KEY, -- UUID
h_order_pk VARCHAR not null, -- UUID
status varchar not null,
load_dt timestamp not null,
load_src varchar not null,
CONSTRAINT fk_order
   FOREIGN KEY(h_order_pk) 
      REFERENCES dds.h_order(h_order_pk));


-- Link-tables --

--drop table if exists dds.l_order_product;
--drop table if exists dds.l_product_restaurant;
--drop table if exists dds.l_product_category;
--drop table if exists dds.l_order_user;


create table if not exists dds.l_order_product
(
hk_order_product_pk VARCHAR PRIMARY KEY,
h_order_pk VARCHAR  not null ,
h_product_pk VARCHAR  not null,
load_dt timestamp not null,
load_src varchar not null,
CONSTRAINT fk_order
   FOREIGN KEY(h_order_pk) 
      REFERENCES dds.h_order(h_order_pk),
CONSTRAINT fk_product
   FOREIGN KEY(h_product_pk) 
      REFERENCES dds.h_product(h_product_pk)
);

create table if not exists dds.l_product_restaurant
(
hk_product_restaurant_pk VARCHAR PRIMARY KEY,
h_restaurant_pk VARCHAR  not null ,
h_product_pk VARCHAR  not null,
load_dt timestamp not null,
load_src varchar not null,
CONSTRAINT fk_restaurant_pr
   FOREIGN KEY(h_restaurant_pk) 
      REFERENCES dds.h_restaurant(h_restaurant_pk),
CONSTRAINT fk_product_1
   FOREIGN KEY(h_product_pk) 
      REFERENCES dds.h_product(h_product_pk)
);

create table if not exists dds.l_product_category
(
hk_product_category_pk VARCHAR PRIMARY KEY,
h_category_pk VARCHAR  not null ,
h_product_pk VARCHAR  not null,
load_dt timestamp not null,
load_src varchar not null,
CONSTRAINT fk_h_category
   FOREIGN KEY(h_category_pk) 
      REFERENCES dds.h_category(h_category_pk),
CONSTRAINT fk_product_1
   FOREIGN KEY(h_product_pk) 
      REFERENCES dds.h_product(h_product_pk)
);

create table if not exists dds.l_order_user
(
hk_order_user_pk VARCHAR PRIMARY KEY,
h_order_pk VARCHAR  not null, 
h_user_pk VARCHAR  not null,
load_dt timestamp not null,
load_src varchar not null,
CONSTRAINT fk_h_user
   FOREIGN KEY(h_user_pk) 
      REFERENCES dds.h_user(h_user_pk),
CONSTRAINT fk_order
   FOREIGN KEY(h_order_pk) 
      REFERENCES dds.h_order(h_order_pk)
);
