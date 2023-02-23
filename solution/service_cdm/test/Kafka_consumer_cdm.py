# Проверяем есть ли данные в dds_service_orders
from kafka import KafkaConsumer
import json
# Крайняя мера, если никак не выйдет запустить stg
consumer = KafkaConsumer(
    'cdm_service_orders',
    bootstrap_servers='rc1a-acu020ec9kbesc8g.mdb.yandexcloud.net:9091',
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-512",
    group_id="None",
    auto_offset_reset='earliest',
    sasl_plain_username='producer_consumer',
    sasl_plain_password='12345678',
    ssl_cafile="/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt")

print("ready")

for v in consumer:
    print(json.loads(v.value))


