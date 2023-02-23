from datetime import datetime
from logging import Logger
import json
from .repository.dds_repository import DdsRepository
from .dds_emulations import ToDdsKafkaProducer, FromDdsKafkaProducer

class DdsMessageProcessor:
    def __init__(self,
                consumer,
                producer,
                dds_repository,
                logger: Logger) -> None:

        self._logger = logger
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._batch_size = 30

    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f"{datetime.utcnow()}: START")
        # обработка сообщений. Создаем обхект message класса StgRepository
        message = DdsRepository(self._dds_repository)
        # В цикле обрабатываем данные по величине self._batch_size
        for _ in range(self._batch_size):
            # получаем данные из consumer
            order_data = ToDdsKafkaProducer.produce() #order_data = self._consumer.consume() консьюмер заменен на эмулятор
            #scr = self._consumer.kafka_consumer_topic
            # проверка на пустое сообщение
            if not order_data:
                self._logger.info('Данные закончились')
                break
            # если заказ с тем же номером и таким же статусом уже есть, то пропускаем, это дубль
            if message.check(order = order_data['object_id'], status = order_data['payload']['status']):
                self._logger.info('Обнаружен дубль')
                continue
            # обращаемся к функциям объекта message и записываем данные в dds слой postgres
            message.load_user(
                user_data = order_data['payload']['user'])
            message.load_products(
                products_data = order_data['payload']['products'])
            message.load_restaurant(
                restaurant_data = order_data["payload"]['restaurant'])
            message.load_orders(
                payload_data = order_data['payload'])
            message.load_links(
                order_data = order_data)
            self._logger.info('Данные загружены в POSTGRES')
            # нам нужно передать только часть данных в новый топик для построения витрин в cdm
            # для счетчика заказов по блюдам нужны id заказа и название блюда order_data["payload"]["products"]
            # для счётчика заказов по категориям товаров order_data["payload"]["products"]
            # значит достаточно передать order_data["payload"]["products"] - список словарей и данные по клиенту для каждого продукта
            data_out = order_data["payload"]["products"]
            for record in data_out:
                record['user_id'] = order_data["payload"]['user']['id']

            #Отправляем сформированные данные в Kafka 
            #self._producer.produce(data_out) не отправляет(((
            
            # эмуляция отправки данных в кафку
            producer = FromDdsKafkaProducer()
            print(data_out)
            producer.send(data_out)

        self._logger.info(f"{datetime.utcnow()}: FINISH")

