--- HUB-tables ---
--drop table if exists dds.h_user;
create table if not exists dds.h_user
(
h_user_pk UUID PRIMARY KEY,
user_id VARCHAR  not null,
load_dt timestamp not null,
load_src varchar not null
)
--drop table if exists dds.h_product;
create table if not exists dds.h_product
(
h_product_pk UUID PRIMARY KEY,
product_id VARCHAR  not null,
load_dt timestamp not null,
load_src varchar not null
)
--drop table if exists dds.h_category;
create table if not exists dds.h_category
(
h_category_pk UUID PRIMARY KEY,
category_name VARCHAR  not null,
load_dt timestamp not null,
load_src varchar not null
)
--drop table if exists dds.h_restaurant;
create table if not exists dds.h_restaurant
(
h_restaurant_pk UUID PRIMARY KEY,
restaurant_id VARCHAR  not null,
load_dt timestamp not null,
load_src varchar not null
)
--drop table if exists dds.h_order;
create table if not exists dds.h_order
(
h_order_pk UUID PRIMARY KEY,
order_id integer  not null,
order_dt timestamp  not null,
load_dt timestamp not null,
load_src varchar not null
)

-- Link-tables --

--drop table if exists dds.l_order_product;
create table if not exists dds.l_order_product
(
hk_order_product_pk UUID PRIMARY KEY,
h_order_pk uuid  not null ,
h_product_pk uuid  not null,
load_dt timestamp not null,
load_src varchar not null,
CONSTRAINT fk_order
   FOREIGN KEY(h_order_pk) 
      REFERENCES dds.h_order(h_order_pk),
CONSTRAINT fk_product
   FOREIGN KEY(h_product_pk) 
      REFERENCES dds.h_product(h_product_pk)
)

--drop table if exists dds.l_product_restaurant;
create table if not exists dds.l_product_restaurant
(
hk_product_restaurant_pk UUID PRIMARY KEY,
h_restaurant_pk uuid  not null ,
h_product_pk uuid  not null,
load_dt timestamp not null,
load_src varchar not null,
CONSTRAINT fk_restaurant_pr
   FOREIGN KEY(h_restaurant_pk) 
      REFERENCES dds.h_restaurant(h_restaurant_pk),
CONSTRAINT fk_product_1
   FOREIGN KEY(h_product_pk) 
      REFERENCES dds.h_product(h_product_pk)
)

--drop table if exists dds.l_product_category;
create table if not exists dds.l_product_category
(
hk_product_category_pk UUID PRIMARY KEY,
h_category_pk uuid  not null ,
h_product_pk uuid  not null,
load_dt timestamp not null,
load_src varchar not null,
CONSTRAINT fk_h_category
   FOREIGN KEY(h_category_pk) 
      REFERENCES dds.h_category(h_category_pk),
CONSTRAINT fk_product_1
   FOREIGN KEY(h_product_pk) 
      REFERENCES dds.h_product(h_product_pk)
)

--drop table if exists dds.l_order_user;
create table if not exists dds.l_order_user
(
hk_order_user_pk UUID PRIMARY KEY,
h_order_pk uuid  not null, 
h_user_pk uuid  not null,
load_dt timestamp not null,
load_src varchar not null,
CONSTRAINT fk_h_user
   FOREIGN KEY(h_user_pk) 
      REFERENCES dds.h_user(h_user_pk),
CONSTRAINT fk_order
   FOREIGN KEY(h_order_pk) 
      REFERENCES dds.h_order(h_order_pk)
)

-- Satelit-tables --

--drop table if exists dds.s_user_names;
create table if not exists dds.s_user_names
(
hk_user_names_pk UUID PRIMARY KEY,
h_user_pk uuid  not null,
username varchar not null,
userlogin varchar not null,
load_dt timestamp not null,
load_src varchar not null,
CONSTRAINT fk_h_user
   FOREIGN KEY(h_user_pk) 
      REFERENCES dds.h_user(h_user_pk))

--drop table if exists dds.s_product_names;
create table if not exists dds.s_product_names
(
hk_product_names_pk UUID PRIMARY KEY,
h_product_pk uuid  not null,
name varchar not null,
load_dt timestamp not null,
load_src varchar not null,
CONSTRAINT fk_h_product
   FOREIGN KEY(h_product_pk) 
      REFERENCES dds.h_product(h_product_pk))

--drop table if exists dds.s_restaurant_names;
create table if not exists dds.s_restaurant_names
(
hk_restaurant_names_pk UUID PRIMARY KEY,
h_restaurant_pk uuid  not null,
name varchar not null,
load_dt timestamp not null,
load_src varchar not null,
CONSTRAINT fk_h_restaurant
   FOREIGN KEY(h_restaurant_pk) 
      REFERENCES dds.h_restaurant(h_restaurant_pk))

--drop table if exists dds.s_order_cost;
create table if not exists dds.s_order_cost
(
hk_order_cost_pk UUID PRIMARY KEY,
h_order_pk uuid  not null, 
cost decimal(18,5) not null,
payment decimal(18,5) not null,
load_dt timestamp not null,
load_src varchar not null,
CONSTRAINT fk_order
   FOREIGN KEY(h_order_pk) 
      REFERENCES dds.h_order(h_order_pk))

--drop table if exists dds.s_order_status;
create table if not exists dds.s_order_status
(
hk_order_status_pk UUID PRIMARY KEY,
h_order_pk uuid  not null, 
status varchar not null,
load_dt timestamp not null,
load_src varchar not null,
CONSTRAINT fk_order
   FOREIGN KEY(h_order_pk) 
      REFERENCES dds.h_order(h_order_pk))
