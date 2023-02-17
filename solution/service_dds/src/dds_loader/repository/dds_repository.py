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
        user_data = {
                "id": "626a81ce9a8cd1920641e296",
                "name": "Котова Ольга Вениаминовна"}
        """
        with self._db.connection() as c:
            c.cursor().execute(
                    f"""
                        INSERT INTO dds.h_user (h_user_pk, user_id, load_dt, load_src) VALUES
                        (%(h_user_pk)s, %(order_data)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT h_user_pk DO NOTHING -- это пока заготовка!!!!!!!!!
                    """,
                    {
                        'h_user_pk': hash(user_data['id']),
                        'user_id': user_data['id'],
                        'load_dt': datetime.now(),
                        'load_src': 'А вот тут вопрос что указывать в качестве источника!!!!!!!!!',
                    })
            
            hk_user_names_pk = 'Специально вынес это поле отдельно, так как немного непонятно как оно должно быть сгенерировано'
            userlogin = 'userlogin есть в DDL коде урока, но я не вижу этих данных'
            c.cursor().execute(
                    f"""
                        INSERT INTO dds.s_user_names (hk_user_names_pk, h_user_pk, username, userlogin, load_dt, load_src) VALUES
                        (%(hk_user_names_pk)s, %(h_user_pk)s, %(username)s, %(userlogin)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT hk_user_names_pk DO NOTHING -- это пока заготовка!!!!!!!!!
                    """,
                    {
                        'hk_user_names_pk': hk_user_names_pk,
                        'h_user_pk': user_data['id'],
                        'username': user_data['name'],
                        'userlogin': userlogin,
                        'load_dt': datetime.now(),
                        'load_src': 'А вот тут вопрос что указывать в качестве источника!!!!!!!!!',
                    })

    def load_products(self,
                            products_data: dict # products_data = order_data['payload']['products']:
                            ) -> None:
        """
        Принимает в себя данные вида 
        products_data = [
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
        """
        for record in products_data:
            with self._db.connection() as c:
                c.cursor().execute(
                    f"""
                        INSERT INTO dds.h_product (h_product_pk, product_id, load_dt, load_src) VALUES
                        (%(hk_user_names_pk)s, %(h_user_pk)s, %(username)s, %(userlogin)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT h_product_pk DO NOTHING -- это пока заготовка!!!!!!!!!
                    """,
                    {
                        'h_product_pk': hash(record['id']),
                        'product_id': record['id'],
                        'load_dt': datetime.now(),
                        'load_src': 'А вот тут вопрос что указывать в качестве источника!!!!!!!!!',
                    })
                hk_product_names_pk = 'Специально вынес это поле отдельно, так как немного непонятно как оно должно быть сгенерировано'
                c.cursor().execute(
                    f"""
                        INSERT INTO dds.s_product_names (hk_product_names_pk, h_product_pk, name, load_dt, load_src) VALUES
                        (%(hk_user_names_pk)s, %(h_user_pk)s, %(username)s, %(userlogin)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT hk_product_names_pk DO NOTHING -- это пока заготовка!!!!!!!!!
                    """,
                    {
                        'hk_product_names_pk': hk_product_names_pk,
                        'h_product_pk': hash(record['id']),
                        'name': record['name'],
                        'load_dt': datetime.now(),
                        'load_src': 'А вот тут вопрос что указывать в качестве источника!!!!!!!!!',
                    })                
                # чтобы не прогонять лишнюю итерацию сразу запишем категории
                h_category_pk = 'Специально вынес это поле отдельно, так как немного непонятно как оно должно быть сгенерировано'
                c.cursor().execute(
                    f"""
                        INSERT INTO dds.h_category (h_category_pk, category_name, load_dt, load_src) VALUES
                        (%(h_category_pk)s, %(category_name)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT h_category_pk DO NOTHING -- это пока заготовка!!!!!!!!!
                    """,
                    {
                        'h_category_pk': h_category_pk,
                        'category_name': record["category"],
                        'load_dt': datetime.now(),
                        'load_src': 'А вот тут вопрос что указывать в качестве источника!!!!!!!!!',
                    })
    
    
    def load_restaurant(self,
                            restaurant_data: dict # restaurant_data = order_data["payload"]['restaurant']
                            ) -> None:
        """
        Принимает в себя данные вида
        restaurant_data = {
                    "id": "626a81cfefa404208fe9abae",
                    "name": "Кофейня №1"
                        }
        """
        with self._db.connection() as c:
            c.cursor().execute(
                    f"""
                        INSERT INTO dds.h_restaurant (h_restaurant_pk, restaurant_id, load_dt, load_src) VALUES
                        (%(h_restaurant_pk)s, %(restaurant_id)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT h_category_pk DO NOTHING -- это пока заготовка!!!!!!!!!
                    """,
                    {
                        'h_category_pk': hash(restaurant_data["id"]),
                        'restaurant_id': restaurant_data["id"],
                        'load_dt': datetime.now(),
                        'load_src': 'А вот тут вопрос что указывать в качестве источника!!!!!!!!!',
                    })
                
        with self._db.connection() as c:
            hk_restaurant_names_pk = 'Специально вынес это поле отдельно, так как немного непонятно как оно должно быть сгенерировано'
            c.cursor().execute(
                    f"""
                        INSERT INTO dds.s_restaurant_names (hk_restaurant_names_pk, h_restaurant_pk, name, load_dt, load_src) VALUES
                        (%(hk_restaurant_names_pk)s, %(h_restaurant_pk)s, %(name)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT hk_restaurant_names_pk DO NOTHING -- это пока заготовка!!!!!!!!!
                    """,
                    {
                        'hk_restaurant_names_pk': hk_restaurant_names_pk,
                        'h_category_pk': hash(restaurant_data["id"]),
                        'h_category_pk': restaurant_data["name"],
                        'restaurant_id': restaurant_data["id"],
                        'load_dt': datetime.now(),
                        'load_src': 'А вот тут вопрос что указывать в качестве источника!!!!!!!!!',
                    })


    def load_orders(self,
                            payload_data: dict # payload_data = order_data['payload']
                            ) -> None:
        """
        Принимает в себя данные вида
        payload_data = {
                "id": 322519,
                "date": "2022-11-19 16:06:36",
                "cost": 300,
                "payment": 300,
                "status": "CLOSED",
                "restaurant": {
                    "id": "626a81cfefa404208fe9abae",
                    "name": "Кофейня №1"
                }
        """
        with self._db.connection() as c:
            c.cursor().execute(
                    f"""
                        INSERT INTO dds.h_order (h_order_pk, order_id, order_dt, load_dt, load_src) VALUES
                        (%(h_order_pk)s, %(order_id)s, %(order_id)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT h_order_pk DO NOTHING -- это пока заготовка!!!!!!!!!
                    """,
                    {
                        'h_order_pk': hash(payload_data['id']),
                        'order_id': payload_data['id'],
                        'order_dt': payload_data["date"],
                        'load_dt': datetime.now(),
                        'load_src': 'А вот тут вопрос что указывать в качестве источника!!!!!!!!!',
                    })
        with self._db.connection() as c:
            hk_order_cost_pk = 'hk_order_cost_pk'
            c.cursor().execute(
                    f"""
                        INSERT INTO dds.dds.s_order_cost (hk_order_cost_pk, h_order_pk, cost, payment, load_dt, load_src) VALUES
                        (%(hk_order_cost_pk)s, %(h_order_pk)s, %(cost)s, %(payment)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT hk_order_cost_pk DO NOTHING -- это пока заготовка!!!!!!!!!
                    """,
                    {
                        'hk_order_cost_pk': hk_order_cost_pk,
                        'h_order_pk': hash(payload_data['id']),
                        'order_id': payload_data['cost'],
                        'order_dt': payload_data['payment'],
                        'load_dt': datetime.now(),
                        'load_src': 'А вот тут вопрос что указывать в качестве источника!!!!!!!!!',
                    })
        with self._db.connection() as c:
            hk_order_status_pk = 'hk_order_status_pk'
            c.cursor().execute(
                    f"""
                        INSERT INTO dds.dds.s_order_cost (hk_order_status_pk, h_order_pk, status, load_dt, load_src) VALUES
                        (%(hk_order_status_pk)s, %(h_order_pk)s, %(cost)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT hk_order_status_pk DO NOTHING -- это пока заготовка!!!!!!!!!
                    """,
                    {
                        'hk_order_status_pk': hk_order_status_pk,
                        'h_order_pk': hash(payload_data['id']),
                        'status': payload_data['status'],
                        'load_dt': datetime.now(),
                        'load_src': 'А вот тут вопрос что указывать в качестве источника!!!!!!!!!',
                    })
    
    # Здесь есть нюанс с заполнением вложеных сущностей
    
    def load_links(self, order_data
                            ) -> None:
        hk_order_status_pk = 'генерим'
        c.cursor().execute(
                    f"""
                        INSERT INTO dds.dds.l_order_product (hk_order_product_pk, h_order_pk, h_product_pk, load_dt, load_src) VALUES
                        (%(hk_order_status_pk)s, %(h_order_pk)s, %(cost)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT hk_order_status_pk DO NOTHING -- это пока заготовка!!!!!!!!!
                    """,
                    {
                        'hk_order_status_pk': hk_order_status_pk,
                        'h_order_pk': hash(order_data['payload']['id']),
                        'h_product_pk': hash(order_data['payload']['products']['id']),
                        'load_dt': datetime.now(),
                        'load_src': 'А вот тут вопрос что указывать в качестве источника!!!!!!!!!',
                    })
        hk_product_restaurant_pk = 'генерим'
        c.cursor().execute(
                    f"""
                        INSERT INTO dds.dds.l_product_restaurant (hk_product_restaurant_pk, h_restaurant_pk, h_product_pk, load_dt, load_src) VALUES
                        (%(hk_order_status_pk)s, %(h_order_pk)s, %(cost)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT hk_order_status_pk DO NOTHING -- это пока заготовка!!!!!!!!!
                    """,
                    {
                        'hk_order_status_pk': hk_product_restaurant_pk,
                        'h_restaurant_pk': hash(order_data["payload"]['restaurant']),
                        'h_product_pk': hash(order_data['payload']['products']['id']),
                        'load_dt': datetime.now(),
                        'load_src': 'А вот тут вопрос что указывать в качестве источника!!!!!!!!!',
                    })
        hk_product_category_pk = 'генерим'
        c.cursor().execute(
                    f"""
                        INSERT INTO dds.dds.l_product_category (hk_product_category_pk, h_category_pk, h_product_pk, load_dt, load_src) VALUES
                        (%(hk_order_status_pk)s, %(h_order_pk)s, %(cost)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT hk_order_status_pk DO NOTHING -- это пока заготовка!!!!!!!!!
                    """,
                    {
                        'hk_product_category_pk': hk_product_category_pk,
                        'h_category_pk': hash(order_data["payload"]['restaurant']),
                        'h_product_pk': hash(order_data['payload']['products']['category']),
                        'load_dt': datetime.now(),
                        'load_src': 'А вот тут вопрос что указывать в качестве источника!!!!!!!!!',
                    })
        hk_order_user_pk = 'генерим'
        c.cursor().execute(
                    f"""
                        INSERT INTO dds.dds.l_order_user (hk_order_user_pk, h_order_pk, h_user_pk, load_dt, load_src) VALUES
                        (%(hk_order_status_pk)s, %(h_order_pk)s, %(cost)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT hk_order_status_pk DO NOTHING -- это пока заготовка!!!!!!!!!
                    """,
                    {
                        'hk_order_user_pk': hk_order_user_pk,
                        'h_order_pk': hash(order_data['payload']['id']),
                        'h_product_pk': hash(order_data['payload']['user']),
                        'load_dt': datetime.now(),
                        'load_src': 'А вот тут вопрос что указывать в качестве источника!!!!!!!!!',
                    })


# Важно выполнить это в цикле!!!! по количеству категорий товаров

