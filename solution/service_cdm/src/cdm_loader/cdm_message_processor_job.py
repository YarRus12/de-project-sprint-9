from datetime import datetime
from logging import Logger
from uuid import UUID
from .repository.cdm_repository import CdmRepository
from lib.kafka_connect import KafkaConsumer


class CdmMessageProcessor:
    def __init__(self,
                 dds_repository,
                 logger: Logger,
                 ) -> None:

        self._logger = logger
        # видимо мне нужно сделать одним из аттрибутов класса объект другого класса
        self._consumer = KafkaConsumer(
                            host = '',
                            port= '',
                            user= '',
                            password = '',
                            topic = '',
                            group = '',
                            cert_path = '')
        self._dds_repository = dds_repository
        self._batch_size = 100

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")
        message = CdmRepository(self._dds_repository)
        for _ in range(self._batch_size):
            # получаем данные из consumer
            data = self._consumer.consume()
            # проверка на пустое сообщение
            if not data:
                break
            message.load_data(
                data = data, ensure_ascii=False)

        self._logger.info(f"{datetime.utcnow()}: FINISH")
