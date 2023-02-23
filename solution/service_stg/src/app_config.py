import os
# from dotenv import load_dotenv
#from kafka import KafkaConsumer
from lib.kafka_connect import KafkaProducer, KafkaConsumer
from lib.redis import RedisClient
from lib.pg import PgConnect


cert_path='/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt'



# load_dotenv()

class AppConfig:
    CERTIFICATE_PATH = '/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt'
    DEFAULT_JOB_INTERVAL = 25

    def __init__(self) -> None:

        self.kafka_host = "rc1a-acu020ec9kbesc8g.mdb.yandexcloud.net:9091"
        self.kafka_port = "9091"
        self.kafka_consumer_username = "producer_consumer"
        self.kafka_consumer_password = "12345678"
        self.kafka_consumer_group = "None"
        self.kafka_consumer_topic = "order-service_orders"
        self.kafka_producer_username = "producer_consumer"
        self.kafka_producer_password = "12345678"
        self.kafka_producer_topic = "stg-service-orders"
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
            host = self.kafka_host,
            port = self.kafka_port,
            user = self.kafka_consumer_username,
            password = self.kafka_consumer_password,
            topic = self.kafka_consumer_topic,
            group = self.kafka_consumer_group,
            cert_path = self.CERTIFICATE_PATH
        )
    """

    def kafka_consumer(self):
        return KafkaConsumer(
        'stg-service-orders', #  order-service_orders
        bootstrap_servers='rc1a-acu020ec9kbesc8g.mdb.yandexcloud.net:9091',
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-512",
        group_id="None",
        auto_offset_reset='earliest',
        sasl_plain_username='producer_consumer',
        sasl_plain_password='12345678',
        ssl_cafile="/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt")
    """
    def redis_client(self) -> RedisClient:
        return RedisClient(
            self.redis_host,
            self.redis_port,
            self.redis_password,
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
