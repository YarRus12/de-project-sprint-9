from datetime import datetime
from logging import Logger
from uuid import UUID
from .repository.cdm_repository import CdmRepository
from lib.kafka_connect import KafkaConsumer
from .cdm_emulations import ToCdmKafkaProducer


class CdmMessageProcessor:
    def __init__(self,
                consumer,
                cdm_repository,
                logger: Logger,
                ) -> None:

        self._logger = logger
        self._consumer = consumer
        self._cdm_repository = cdm_repository
        self._batch_size = 100

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")
        message = CdmRepository(self._cdm_repository)
        for _ in range(self._batch_size):
            # получаем данные из consumer
            #data = self._consumer.consume()
            # получаем данные из эмулятора
            data = ToCdmKafkaProducer.produce() 
            # проверка на пустое сообщение
            if not data:
                break
            for record in data:
                message.load_data(
                    data = record)

        self._logger.info(f"{datetime.utcnow()}: FINISH")
