import time
from datetime import datetime
from logging import Logger
import json

from .repository.stg_repository import StgRepository


class StgMessageProcessor:
    def __init__(self,
                consumer,
                producer,
                redis,
                stg_repository,
                batch_size,
                logger):
        self._logger = logger
        self._consumer = consumer
        self._producer = producer
        self._redis = redis
        self._stg_repository = stg_repository
        self._batch_size = batch_size


    # функция, которая будет вызываться по расписанию.
    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f"{datetime.utcnow()}: START")

        # обработка сообщений. Создаем обхект message класса StgRepository
        message = StgRepository(self._stg_repository)
        # В цикле обрабатываем данные по величине self._batch_size
        for _ in range(self._batch_size):
            # получаем данные из consumer
            order_data = self._consumer.consume()
            if not order_data:
                break
            # обращаемся к функции order_events_insert объекта message и записываем данные в postgres
            message.order_events_insert(
                object_id = order_data['object_id'],
                object_type = order_data['object_type'],
                sent_dttm = order_data['sent_dttm'],
                payload = json.dumps(order_data['payload'], ensure_ascii=False),
            )
            # получаем данные из redis
            # данные о заказе
            order_data['payload']['user']['name'] = self._redis.get\
                    (order_data['payload']['user']['id'])['name']
            # данные о ресторанах
            restaurant_data = self._redis.get(order_data['payload']['restaurant']['id'])

            # обогащаем данные о заказе данными о ресторане
            order_data['payload']['restaurant']['name'] = restaurant_data['name']

            for order_item in order_data['payload']['order_items']:
                for menu_item in restaurant_data['menu']:
                    if order_item ['id'] == menu_item['_id']:
                        order_item['category'] = menu_item['category']
                        break
            """
                Выходные данные
                    {
            "object_id": 322519,
            "object_type": "order",
            "payload": {
                "id": 322519,
                "date": "2022-11-19 16:06:36",
                "cost": 300,
                "payment": 300,
                "status": "CLOSED",
                "restaurant": {
                    "id": "626a81cfefa404208fe9abae",
                    "name": "Кофейня №1"
                },
                "user": {
                    "id": "626a81ce9a8cd1920641e296",
                    "name": "Котова Ольга Вениаминовна"
                },
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
            }
        }
            """

            # передаем обогащенные данные в kakfa
            self._producer.produce(order_data)

        # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.utcnow()}: FINISH")