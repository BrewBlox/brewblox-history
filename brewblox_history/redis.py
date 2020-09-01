import json
from typing import List

import aioredis
from aiohttp import web
from brewblox_service import brewblox_logger, features, mqtt

LOGGER = brewblox_logger(__name__)

REDIS_HOST = 'redis'
TOPIC = 'brewcast/datastore'


def keycat(namespace: str, key: str) -> str:
    return f'{namespace}:{key}'


def keystrip(namespace: str, key: str) -> str:
    return key[len(namespace)+1:]


def flatten(data: List):
    return [item for sublist in data for item in sublist]


class RedisClient(features.ServiceFeature):

    def __init__(self, app: web.Application):
        super().__init__(app)
        self._redis: aioredis.Redis = None

    async def startup(self, app: web.Application):
        await self.shutdown(app)
        self._redis = await aioredis.create_redis_pool(f'redis://{REDIS_HOST}')

    async def shutdown(self, app: web.Application):
        if self._redis:
            self._redis.close()
            await self._redis.wait_closed()

    async def ping(self):
        return {'ping': await self._redis.ping()}

    async def get(self, namespace: str, key: str) -> dict:
        v = await self._redis.get(keycat(namespace, key))
        return json.loads(v)

    async def mget(self, namespace: str, keys: List[str] = None, filter: str = None) -> List[dict]:
        keys = keys or []
        fkeys = []
        if filter is not None:
            fkeys = await self._redis.keys(keycat(namespace, filter))

        if keys or fkeys:
            values = await self._redis.mget(*[keycat(namespace, key) for key in keys], *fkeys)
        else:
            values = []
        return [json.loads(v) for v in values]

    async def set(self, namespace: str, value: dict) -> dict:
        key = value['id']
        await self._redis.set(keycat(namespace, key), json.dumps(value))
        await mqtt.publish(self.app, f'{TOPIC}/{namespace}', {'changed': [value]})
        return value

    async def mset(self, namespace: str, values: List[dict]) -> List[dict]:
        args = flatten([[keycat(namespace, v['id']), json.dumps(v)] for v in values])
        await self._redis.mset(*args)
        await mqtt.publish(self.app, f'{TOPIC}/{namespace}', {'changed': values})
        return values

    async def delete(self, namespace: str, key: str) -> int:
        count = await self._redis.delete(keycat(namespace, key))
        await mqtt.publish(self.app, f'{TOPIC}/{namespace}', {'deleted': [key]})
        return {'count': count}

    async def mdelete(self, namespace: str, keys: List[str] = None, filter: str = None) -> int:
        keys = [keycat(namespace, key) for key in (keys or [])]
        if filter is not None:
            keys += await self._redis.keys(keycat(namespace, filter))

        if keys:
            count = await self._redis.delete(*keys)
            basekeys = [keystrip(namespace, k) for k in keys]
            await mqtt.publish(self.app, f'{TOPIC}/{namespace}', {'deleted': basekeys})
        else:
            count = 0

        return {'count': count}


def setup(app: web.Application):
    features.add(app, RedisClient(app))


def get_redis(app: web.Application) -> RedisClient:
    return features.get(app, RedisClient)
