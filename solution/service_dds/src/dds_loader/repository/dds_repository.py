import uuid
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect
from pydantic import BaseModel


class DdsRepository:
    
    def load_user(self,
                            user_data: dict # user_data = order_data['user']
                            ) -> None:
        """
        Принимает в себя данные вида 
        "user": {
                "id": "626a81ce9a8cd1920641e296",
                "name": "Котова Ольга Вениаминовна"}
        """
        with self._db.connection() as c:
            h_user_pk = 'Это поле нужно генерировать с помощью специальной рукописной функции c хешированием, так?'
            c.cursor().execute(
                    f"""
                        INSERT INTO dds.h_user (h_user_pk, user_id, load_dt, load_src) VALUES
                        (%(h_user_pk)s, %(order_data)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT {h_user_pk} DO NOTHING -- это пока заготовка!!!!!!!!!
                    """,
                    {
                        'h_user_pk': h_user_pk,
                        'user_id': user_data['id'],
                        'load_dt': datetime.now(),
                        'load_src': 'А вот тут вопрос что указывать в качестве источника!!!!!!!!!',
                    })
            hk_user_names_pk = 'тут точно нужно нарезать какой-то хэш'
            userlogin = 'Опачки! А это откуда взять?!'
            c.cursor().execute(
                    f"""
                        INSERT INTO dds.s_user_names (hk_user_names_pk, h_user_pk, username, userlogin, load_dt, load_src) VALUES
                        (%(hk_user_names_pk)s, %(h_user_pk)s, %(username)s, %(userlogin)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT {h_user_pk} DO NOTHING -- это пока заготовка!!!!!!!!!
                    """,
                    {
                        'hk_user_names_pk': hk_user_names_pk,
                        'h_user_pk': h_user_pk,
                        'username': user_data['name'],
                        'userlogin': userlogin,
                        'load_dt': datetime.now(),
                        'load_src': 'А вот тут вопрос что указывать в качестве источника!!!!!!!!!',
                    })

    def load_products(self,
                            products_data: dict # user_data = order_data['user']
                            ) -> None:
        """
        Принимает в себя данные вида 
        "products": [
                    {
                        "id": "6276e8cd0cf48b4cded00878",
                        "price": 180,
                        "quantity": 1,
                        "name": "РОЛЛ С ТОФУ И ВЯЛЕНЫМИ ТОМАТАМИ",
                        "category": "Выпечка"
                    },
                    {
                        "id": "6276e8cd0cf48b4cded0086c",
                        "price": 60,
                        "quantity": 2,
                        "name": "ГРИЛАТА ОВОЩНАЯ ПО-МЕКСИКАНСКИ",
                        "category": "Закуски"
                    }
                ]
        create table if not exists dds.h_product
        (
        h_product_pk UUID PRIMARY KEY,
        product_id VARCHAR  not null,
        load_dt timestamp not null,
        load_src varchar not null
        )
        """
        pass
    
    def load_category(self,
                            products_data: dict # user_data = order_data['user']
                            ) -> None:
        """
        Принимает в себя данные вида 
        Или нет?! Хм тут нужно будет уточниться 
        "products": [
                    {
                        "id": "6276e8cd0cf48b4cded00878",
                        "price": 180,
                        "quantity": 1,
                        "name": "РОЛЛ С ТОФУ И ВЯЛЕНЫМИ ТОМАТАМИ",
                        "category": "Выпечка"
                    },
                    {
                        "id": "6276e8cd0cf48b4cded0086c",
                        "price": 60,
                        "quantity": 2,
                        "name": "ГРИЛАТА ОВОЩНАЯ ПО-МЕКСИКАНСКИ",
                        "category": "Закуски"
                    }
                ]
        create table if not exists dds.h_category
        (
        h_category_pk UUID PRIMARY KEY,
        category_name VARCHAR  not null,
        load_dt timestamp not null,
        load_src varchar not null
        )
        """
        pass
    
    def load_restaurant(self,
                            products_data: dict # user_data = order_data['user']
                            ) -> None:
        """
        Принимает в себя данные вида order_data['payload']['restaurant']
        "restaurant": {
                    "id": "626a81cfefa404208fe9abae",
                    "name": "Кофейня №1"
                },
        create table if not exists dds.h_restaurant
        (
        h_restaurant_pk UUID PRIMARY KEY,
        restaurant_id VARCHAR  not null,
        load_dt timestamp not null,
        load_src varchar not null
        """
        pass

    def load_orders(self,
                            products_data: dict # user_data = order_data['user']
                            ) -> None:
        """
        Принимает в себя данные вида order_data['payload']
        "restaurant": {
                    "id": "626a81cfefa404208fe9abae",
                    "name": "Кофейня №1"
                },
        create table if not exists dds.h_order
        (
        h_order_pk UUID PRIMARY KEY,
        order_id integer  not null,
        order_dt timestamp  not null,
        load_dt timestamp not null,
        load_src varchar not null
        )
        """
        pass

