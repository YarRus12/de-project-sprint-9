from datetime import datetime
from logging import Logger


class DdsMessageProcessor:
    def __init__(self,
                 logger: Logger) -> None:

        self._logger = logger
        self._batch_size = 30

    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f"{datetime.utcnow()}: START")

        self._logger.info(f"{datetime.utcnow()}: FINISH")


"""
Дополните шаблон сервиса кодом, который будет вычитывать обогащённые данные из топика Kafka.
Допишите код, который будет заполнять релевантные для этого сервиса таблицы.
Спроектируйте выходное сообщение для Kafka и реализуйте его в сервисе.
Допишите код, который будет отправлять выходное сообщение в Kafka. Создайте для этого новый топик и дайте ему название.

"""