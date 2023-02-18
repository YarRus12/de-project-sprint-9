import uuid
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect
from pydantic import BaseModel


class CdmRepository:
    
    def __init__(self, db: PgConnect) -> None:
        self._db = db
        
        def load_data(self,
                    data: dict # user_data = order_data['user']
                    ) -> None:
            """
            Принимает в себя данные вида 
            data = {'id': '6276e8cd0cf48b4cded00878', 'price': 180, 'quantity': 1, 
            'name': 'РОЛЛ С ТОФУ И ВЯЛЕНЫМИ ТОМАТАМИ', 'category': 'Выпечка', 
            'user_id': '626a81ce9a8cd1920641e296'}
            """
            # Если я правильно понимаю, cdm.user_product_counters мы считатаем количество продуктов у конкретного покупателей
            # Вопрос как формировать id для записей? следует ли делать это через sequence
            with self._db.connection() as c:
                c.cursor().execute(
                    f"""
                    INSERT INTO cdm.user_product_counters (user_id, product_id, product_name, order_cnt) VALUES
                    (%(user_id)s, %(product_id)s, %(product_name)s, %(order_cnt)s)
                    ON CONFLICT (user_id, product_id) DO UPDATE SET order_cnt = EXCLUDED.order_cnt + 1;
                """,
                {
                    'user_id': data['user_id'],
                    'product_id': data['id'],
                    'product_name':data['name'],
                    'order_cnt': 1,
                })
            with self._db.connection() as c:
                # Вопрос как формировать id для записей? следует ли делать это через sequence
                c.cursor().execute(
                    f"""
                    INSERT INTO cdm.user_category_counters (user_id, category_id, category_name, order_cnt) VALUES
                    (%(user_id)s, %(category_id)s, %(category_name)s, %(order_cnt)s)
                    ON CONFLICT (user_id, category_id) DO UPDATE SET order_cnt = EXCLUDED.order_cnt + 1;
                """,
                {
                    'user_id': data['user_id'],
                    'category_id': hash(data["category"]),
                    'category_name': data['category'],
                    'order_cnt': data['quantity'],
                })