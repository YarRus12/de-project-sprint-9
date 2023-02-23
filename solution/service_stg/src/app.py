import logging
import json
import time
from kafka import KafkaConsumer
from logging import Logger
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask

from app_config import AppConfig
from stg_loader.stg_message_processor_job import StgMessageProcessor

app = Flask(__name__)


# Заводим endpoint для проверки, поднялся ли сервис.
# Обратиться к нему можно будет GET-запросом по адресу localhost:8081/health.
# Если в ответе будет healthy - сервис поднялся и работает.
@app.get('/health')
def health():
    return 'Flask работает'


if __name__ == '__main__':
    # Устанавливаем уровень логгирования в Debug, чтобы иметь возможность просматривать отладочные логи.
    app.logger.setLevel(logging.DEBUG)
    # Инициализируем конфиг. Для удобства, вынесли логику получения значений переменных окружения в отдельный класс.
    config = AppConfig()
    
    consumer = KafkaConsumer(
    'order-service_orders',
    bootstrap_servers='rc1a-acu020ec9kbesc8g.mdb.yandexcloud.net:9091',
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-512",
    group_id="None",
    auto_offset_reset='earliest',
    sasl_plain_username='producer_consumer',
    sasl_plain_password='12345678',
    ssl_cafile="/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt")
    # Инициализируем процессор сообщений.
    # Пока он пустой. Нужен для того, чтобы потом в нем писать логику обработки сообщений из Kafka.
    proc = StgMessageProcessor(
        consumer,
        config.kafka_producer(),
        config.redis_client(),
        config.pg_warehouse_db(),
        100,
        app.logger,
    )

    # Запускаем процессор в бэкграунде.
    # BackgroundScheduler будет по расписанию вызывать функцию run нашего обработчика(SampleMessageProcessor).
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=proc.run, trigger="interval", seconds=50)
    scheduler.start()

    # стартуем Flask-приложение.
    app.run(debug=True, host='127.0.0.1', port=8081, use_reloader=False)
