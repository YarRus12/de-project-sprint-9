import uuid
from datetime import datetime
from typing import Any, Dict, List

import hashlib
from lib.pg import PgConnect
from pydantic import BaseModel


class CdmRepository:
    
    def __init__(self, db: PgConnect) -> None:
        self._db = db
        
    def load_data(self,
                data: dict
                    ) -> None:
            """
            Принимает в себя данные вида 
            data = {'id': '6276e8cd0cf48b4cded00878', 'price': 180, 'quantity': 1, 
            'name': 'РОЛЛ С ТОФУ И ВЯЛЕНЫМИ ТОМАТАМИ', 'category': 'Выпечка', 
            'user_id': '626a81ce9a8cd1920641e296'}
            """
            
            with self._db.connection() as connect:
                max_id=connect.cursor().execute("""SELECT coalesce(MAX(id),0) FROM cdm.user_product_counters""").fetchone()[0]
                connect.cursor().execute(
                    f"""
                    INSERT INTO cdm.user_product_counters (id, user_id, product_id, product_name, order_cnt) VALUES
                    ({max_id+1},'{uuid.uuid5(uuid.NAMESPACE_DNS, data['user_id'])}', '{uuid.uuid5(uuid.NAMESPACE_DNS, data['id'])}', '{data['name']}', 1)
                    ON CONFLICT (user_id, product_id) DO UPDATE SET order_cnt = user_product_counters.order_cnt + 1;
                    """)
                connect.commit()
            with self._db.connection() as connect:
                # Вопрос как формировать id для записей? следует ли делать это через sequence
                max_id=connect.cursor().execute("""SELECT coalesce(MAX(id),0) FROM cdm.user_category_counters""").fetchone()[0]
                sql = f"""
                    INSERT INTO cdm.user_category_counters (id, user_id, category_id, category_name, order_cnt) VALUES
                    ({max_id+1},'{uuid.uuid5(uuid.NAMESPACE_DNS, data['user_id'])}', '{uuid.uuid5(uuid.NAMESPACE_DNS, data['category'])}', '{data['category']}', 1)
                    ON CONFLICT (user_id, category_id) DO UPDATE SET order_cnt = user_category_counters.order_cnt + 1;
                """
                print(sql)
                connect.cursor().execute(sql)
                connect.commit()