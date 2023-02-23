from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers=['rc1a-acu020ec9kbesc8g.mdb.yandexcloud.net:9091'],
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-512",
    sasl_plain_username='producer_consumer',
    sasl_plain_password='12345678',
    ssl_cafile="/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt")


data = {
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


producer.send('dds_service_orders', json.dumps(data).encode('utf-8'), b'16789012kj1g1h')
producer.flush()
producer.close()