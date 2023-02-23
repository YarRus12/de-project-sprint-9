from lib.redis.redis_client import RedisClient
import json
import json
from typing import Dict
import redis

host = 'c-c9qimb312c96m0d2e29m.rw.mdb.yandexcloud.net'
port = '6380'
password = '12345678'
ca_path = '/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt'

class RedisClient:
    def __init__(self, host: str, port: int, password: str, cert_path: str) -> None:
        self._client = redis.StrictRedis(
            host=host,
            port=port,
            password=password,
            ssl=True,
            ssl_ca_certs=cert_path
            )
              
    def set(self, key:str, v: Dict):
        self._client.set(key, json.dumps(v))
    def get(self, k:str) -> Dict:
        return json.loads(self._client.get(f"{k}"))