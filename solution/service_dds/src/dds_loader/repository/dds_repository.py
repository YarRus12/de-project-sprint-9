import uuid
import hashlib
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect
from pydantic import BaseModel
from ..dds_emulations import ToDdsKafkaProducer


class DdsRepository:
    
    def __init__(self, db: PgConnect) -> None:
        self._db = db
        self.load_src = ToDdsKafkaProducer.__name__

    def check(self, order, status) -> bool:
        with self._db.connection() as c:
            check = c.execute(f"""SELECT
                        h_order.order_id
                    FROM dds.h_order
                    INNER JOIN dds.s_order_status
                    ON h_order.h_order_pk = s_order_status.h_order_pk
                    WHERE h_order.order_id = '{order}' 
                    AND s_order_status.status = '{status}';
                """)
            # Если что-то найдено в базе, то это дубль возвращаем True и итерация будет пропущена
            if check.fetchone() is not None:
                return True

    def load_user(self,
                    user_data: dict
                    ) -> None:
        """
        Функция загружает данные о пользователях в dds слой
        Принимает в себя данные вида:
        user_data = order_data[]['user']
        user_data = {
                "id": "626a81ce9a8cd1920641e296",
                "name": "Котова Ольга Вениаминовна"}
        """
        with self._db.connection() as connect:
            h_user_pk = hashlib.sha224(bytes(user_data['id'], 'utf-8')).hexdigest() #hash(user_data['id']) генерит рандомное число, а это не пойдет
            connect.cursor().execute(f"""
                        INSERT INTO dds.h_user (h_user_pk, user_id, load_dt, load_src) VALUES
                        (cast('{h_user_pk}' as VARCHAR), cast('{user_data['id']}' as varchar), '{datetime.now()}'::timestamp, cast('{self.load_src}' as VARCHAR))
                        ON CONFLICT (h_user_pk) DO NOTHING -- допустим у нас уже есть такой пользователь, чтобы скрипт отработал просто ничего не делаем
                    """)
            connect.commit() # Если сейчам не закомитим сателлит не примет h_user_pk
            
            # Вставляем данные в саттелит
            userlogin = 'someuserlogin' # userlogin есть в DDL коде урока, но я не вижу этих данных
            hk_user_names_pk = hashlib.sha224(bytes(user_data['id'] + user_data['name'] + userlogin, 'utf-8')).hexdigest()
            connect.cursor().execute(
                    f"""
                        INSERT INTO dds.s_user_names (hk_user_names_pk, h_user_pk, username, userlogin, load_dt, load_src) VALUES
                        (cast('{hk_user_names_pk}' as VARCHAR), cast('{h_user_pk}' as VARCHAR), cast('{user_data['name']}' as VARCHAR), '{userlogin}', '{datetime.now()}'::timestamp, cast('{self.load_src}' as VARCHAR))
                        ON CONFLICT (hk_user_names_pk) DO NOTHING -- допустим  унас уже есть такой пользователь, чтобы скрипт отработал просто ничего не делаем
                    """)
            connect.commit()


    def load_products(self,
                            products_data: dict # products_data = order_data['payload']['products']:
                            ) -> None:
        """
        Функция загружает данные о всех продуктах и категориях заказа в dds слой
        Принимает в себя данные вида:
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
            with self._db.connection() as connect:
                # Вставляем данные в хаб
                h_product_pk = hashlib.sha224(bytes(record['id'], 'utf-8')).hexdigest() #hash(user_data['id']) генерит рандомное число, а это не пойдет
                connect.cursor().execute(
                    f"""
                        INSERT INTO dds.h_product (h_product_pk, product_id, load_dt, load_src) VALUES
                        (cast('{h_product_pk}' as VARCHAR), cast('{record['id']}' as varchar), '{datetime.now()}'::timestamp, cast('{self.load_src}' as VARCHAR))
                        ON CONFLICT (h_product_pk) DO NOTHING
                    """)
                connect.commit()

                # Вставляем данные в саттелит
                
                hk_product_names_pk = hashlib.sha224(bytes(record['id'] + record['name'], 'utf-8')).hexdigest()
                connect.cursor().execute(
                    f"""
                        INSERT INTO dds.s_product_names (hk_product_names_pk, h_product_pk, name, load_dt, load_src) VALUES
                        (cast('{hk_product_names_pk}' as VARCHAR), cast('{h_product_pk}' as VARCHAR), cast('{record['name']}' as varchar), '{datetime.now()}'::timestamp, cast('{self.load_src}' as VARCHAR))
                        ON CONFLICT (hk_product_names_pk) DO UPDATE SET h_product_pk = EXCLUDED.h_product_pk, name=EXCLUDED.name
                    """)                
                # чтобы не прогонять лишнюю итерацию сразу запишем категории
                connect.commit()
                connect.cursor().execute(
                    f"""
                        INSERT INTO dds.h_category (h_category_pk, category_name, load_dt, load_src) VALUES
                        (cast('{hashlib.sha224(bytes(record["category"], 'utf-8')).hexdigest()}'as VARCHAR), cast('{record["category"]}' as VARCHAR), '{datetime.now()}'::timestamp, cast('{self.load_src}' as VARCHAR))
                        ON CONFLICT (h_category_pk) DO NOTHING
                    """)
                connect.commit()
    
    def load_restaurant(self,
                            restaurant_data: dict 
                            ) -> None:
        """
        Функция загружает данные о ресторанах в dds слой
        Принимает в себя данные вида:
        restaurant_data = order_data["payload"]['restaurant']
        restaurant_data = {
                    "id": "626a81cfefa404208fe9abae",
                    "name": "Кофейня №1"
                        }
        """
        with self._db.connection() as connect:
            # Вставляем данные в хаб
            h_restaurant_pk = hashlib.sha224(bytes(restaurant_data['id'], 'utf-8')).hexdigest()
            connect.cursor().execute(
                    f"""
                        INSERT INTO dds.h_restaurant (h_restaurant_pk, restaurant_id, load_dt, load_src) VALUES
                        (cast('{h_restaurant_pk}' as VARCHAR), cast('{restaurant_data['id']}' as VARCHAR), '{datetime.now()}'::timestamp, cast('{self.load_src}' as VARCHAR))
                        ON CONFLICT (h_restaurant_pk) DO NOTHING -- если есть такой ресторан, то ничего не делаем
                    """)
            connect.commit()
                
        with self._db.connection() as connect:
            # Вставляем данные в саттелит
            hk_restaurant_names_pk = hashlib.sha224(bytes(restaurant_data['id'] + restaurant_data['name'], 'utf-8')).hexdigest()
            connect.cursor().execute(
                    f"""
                        INSERT INTO dds.s_restaurant_names (hk_restaurant_names_pk, h_restaurant_pk, name, load_dt, load_src) VALUES
                        (cast('{hk_restaurant_names_pk}' as VARCHAR), cast('{h_restaurant_pk}' as VARCHAR), '{restaurant_data['name']}', '{datetime.now()}'::timestamp, cast('{self.load_src}' as VARCHAR))
                        ON CONFLICT (hk_restaurant_names_pk) DO UPDATE SET name=EXCLUDED.name -- ресторан остался, а наименование сменилось
                    """)
            connect.commit()

    def load_orders(self,
                            payload_data: dict
                            ) -> None:
        """
        Функция загружает данные о заказах в dds слой
        Принимает в себя данные вида
        payload_data = order_data['payload']
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
        with self._db.connection() as connect:
            # Вставляем данные в хаб
            h_order_pk = hashlib.sha224(bytes(str(payload_data['id']), 'utf-8')).hexdigest()
            connect.cursor().execute(
                    f"""
                        INSERT INTO dds.h_order (h_order_pk, order_id, order_dt, load_dt, load_src) VALUES
                        (cast('{h_order_pk}' as VARCHAR), cast('{payload_data['id']}' as int), '{payload_data['date']}'::timestamp, '{datetime.now()}'::timestamp, cast('{self.load_src}' as VARCHAR))
                        ON CONFLICT (h_order_pk) DO NOTHING -- если дубль по id, то пропускаем
                    """)
            connect.commit()

        with self._db.connection() as connect:
            # Вставляем данные в саттелит
            hk_order_cost_pk = hashlib.sha224(bytes(str(payload_data['id']) + str(payload_data['cost'])+str(payload_data['payment']), 'utf-8')).hexdigest()
            connect.cursor().execute(
                    f"""
                        INSERT INTO dds.s_order_cost (hk_order_cost_pk, h_order_pk, cost, payment, load_dt, load_src) VALUES
                        (cast('{hk_order_cost_pk}' as VARCHAR), cast('{h_order_pk}' as VARCHAR), cast('{payload_data['cost']}' as decimal(18,5)), cast('{payload_data['payment']}' as decimal(18,5)), '{datetime.now()}'::timestamp, cast('{self.load_src}' as VARCHAR))
                        ON CONFLICT (hk_order_cost_pk) DO NOTHING -- могут ли меняться данные по заказу? пусть будет нет
                    """)
            connect.commit()
            # Вставляем данные в саттелит
        with self._db.connection() as connect:
            hk_order_status_pk = hashlib.sha224(bytes(str(payload_data['id']) + payload_data['status'], 'utf-8')).hexdigest()
            connect.cursor().execute(
                    f"""
                        INSERT INTO dds.s_order_status (hk_order_status_pk, h_order_pk, status, load_dt, load_src) VALUES
                        (cast('{hk_order_status_pk}' as VARCHAR), cast('{h_order_pk}' as VARCHAR), cast('{payload_data['status']}' as VARCHAR), '{datetime.now()}'::timestamp, cast('{self.load_src}' as VARCHAR))
                        ON CONFLICT (hk_order_status_pk) DO NOTHING -- могут ли меняться данные по заказу? пусть будет нет
                    """)
            connect.commit()
    

    def load_links(self, order_data
                            ) -> None:
        """
        Функция принимает на вход все данные и
                заполняет все таблицы соединения, кроме таблицы соединяющий категории и продукты 
        """
        with self._db.connection() as connect:
            h_order_pk = hashlib.sha224(bytes(str(order_data['payload']['id']), 'utf-8')).hexdigest()
            h_restaurant_pk = hashlib.sha224(bytes(order_data['payload']['restaurant']['id'], 'utf-8')).hexdigest()
            h_user_pk = hashlib.sha224(bytes(order_data['payload']['user']['id'], 'utf-8')).hexdigest()
            hk_order_user_pk = hashlib.sha224(bytes(h_order_pk + h_user_pk, 'utf-8')).hexdigest()
            connect.cursor().execute(f"""
                            INSERT INTO dds.l_order_user (hk_order_user_pk, h_order_pk, h_user_pk, load_dt, load_src) VALUES
                            (cast('{hk_order_user_pk}' as VARCHAR), cast('{h_order_pk}' as VARCHAR), cast('{h_user_pk}' as VARCHAR),'{datetime.now()}'::timestamp, cast('{self.load_src}' as VARCHAR))
                            ON CONFLICT (hk_order_user_pk) DO NOTHING 
                        """)
            connect.commit()
            #Для каждого продукта в заказе
            for i in range(len(order_data['payload']['products'])): 
                h_product_pk = hashlib.sha224(bytes(order_data['payload']['products'][i]['id'], 'utf-8')).hexdigest()
                hk_order_product_pk = hashlib.sha224(bytes(h_order_pk + h_product_pk, 'utf-8')).hexdigest()
                connect.cursor().execute(
                        f"""
                            INSERT INTO dds.l_order_product (hk_order_product_pk, h_order_pk, h_product_pk, load_dt, load_src) VALUES
                            (cast('{hk_order_product_pk}' as VARCHAR), cast('{h_order_pk}' as VARCHAR), cast('{h_product_pk}' as VARCHAR),'{datetime.now()}'::timestamp, cast('{self.load_src}' as VARCHAR))
                            ON CONFLICT (hk_order_product_pk) DO NOTHING -- если дубль, то пропускаем
                        """)
                connect.commit()
                hk_product_restaurant_pk = hashlib.sha224(bytes(h_restaurant_pk + h_product_pk, 'utf-8')).hexdigest()
                connect.cursor().execute(
                        f"""
                            INSERT INTO dds.l_product_restaurant (hk_product_restaurant_pk, h_restaurant_pk, h_product_pk, load_dt, load_src) VALUES
                            (cast('{hk_product_restaurant_pk}' as VARCHAR), cast('{h_restaurant_pk}' as VARCHAR), cast('{h_product_pk}' as VARCHAR),'{datetime.now()}'::timestamp, cast('{self.load_src}' as VARCHAR))
                            ON CONFLICT (hk_product_restaurant_pk) DO NOTHING -- если дубль, то пропускаем
                        """)
                connect.commit()
                h_category_pk = hashlib.sha224(bytes(order_data['payload']['products'][i]['category'], 'utf-8')).hexdigest()
                hk_product_category_pk = hashlib.sha224(bytes(h_category_pk + h_product_pk, 'utf-8')).hexdigest()
                connect.cursor().execute(
                            f"""
                                INSERT INTO dds.l_product_category (hk_product_category_pk, h_category_pk, h_product_pk, load_dt, load_src) VALUES
                                (cast('{hk_product_category_pk}' as VARCHAR), cast('{h_category_pk}' as VARCHAR), cast('{h_product_pk}' as VARCHAR),'{datetime.now()}'::timestamp, cast('{self.load_src}' as VARCHAR))
                                ON CONFLICT (hk_product_category_pk) DO NOTHING -- это пока заготовка!!!!!!!!!
                            """)
                connect.commit()
        