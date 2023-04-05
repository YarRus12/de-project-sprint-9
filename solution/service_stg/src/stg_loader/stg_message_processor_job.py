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
        self._logger.info(f"batch_size = {self._batch_size}")
        # получаем данные из consumer
        all_data = [json.loads(value.value) for value in self._consumer]
        self._logger.info(all_data)
        for i in range(self._batch_size):
            order_data = all_data[i]
            self._logger.info(order_data)
            #order_data = json.loads(self._consumer.value().decode())
            if not order_data:
                break
            # обращаемся к функции order_events_insert объекта message и записываем данные в postgres
            self._logger.info(f"Сейчас будем записывать данные в stage")
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
            
            self._logger.info('READY TO SEND MESSAGE IN KAFKA')
            # передаем обогащенные данные в kakfa
            self._producer.produce(order_data)
        # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.utcnow()}: FINISH")
