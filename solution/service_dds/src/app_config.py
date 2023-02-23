import os

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from lib.pg import PgConnect

cert_path='/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt'

class AppConfig:
    CERTIFICATE_PATH = '/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt'

    def __init__(self) -> None:

        self.kafka_host = "rc1a-acu020ec9kbesc8g.mdb.yandexcloud.net:9091"
        self.kafka_port = "9091"
        self.kafka_consumer_username = "producer_consumer"
        self.kafka_consumer_password = "12345678"
        self.kafka_consumer_group = "None"
        self.kafka_consumer_topic = "dds-service_orders"
        self.kafka_producer_username = "producer_consumer"
        self.kafka_producer_password = "12345678"
        self.kafka_producer_topic = "cdm-service-orders"
        self.redis_host = "c-c9qimb312c96m0d2e29m.rw.mdb.yandexcloud.net"
        self.redis_port = 6380
        self.redis_password = "12345678"
        self.pg_warehouse_host = "rc1b-a5w8u7wzrbvvyzns.mdb.yandexcloud.net"
        self.pg_warehouse_port = 6432
        self.pg_warehouse_dbname = "db1"
        self.pg_warehouse_user = "user1"
        self.pg_warehouse_password = "12345678"

    def kafka_producer(self):
        return KafkaProducer(
            self.kafka_host,
            self.kafka_port,
            self.kafka_producer_username,
            self.kafka_producer_password,
            self.kafka_producer_topic,
            self.CERTIFICATE_PATH
        )

    def kafka_consumer(self):
        return KafkaConsumer(
            self.kafka_host,
            self.kafka_port,
            self.kafka_consumer_username,
            self.kafka_consumer_password,
            self.kafka_consumer_topic,
            self.kafka_consumer_group,
            self.CERTIFICATE_PATH
        )

    def pg_warehouse_db(self):
        return PgConnect(
            self.pg_warehouse_host,
            self.pg_warehouse_port,
            self.pg_warehouse_dbname,
            self.pg_warehouse_user,
            self.pg_warehouse_password
        )
