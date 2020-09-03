import json
from typing import List, Optional

import aioredis
from aiohttp import web
from brewblox_service import brewblox_logger, features, mqtt

LOGGER = brewblox_logger(__name__)

REDIS_URL = 'redis://redis'
TOPIC = 'brewcast/datastore'


def keycat(namespace: str, key: str) -> str:
    return f'{namespace}:{key}' if namespace else key


def flatten(data: List):
    return [item for sublist in data for item in sublist]


class RedisClient(features.ServiceFeature):

    def __init__(self, app: web.Application):
        super().__init__(app)
        self._redis: aioredis.Redis = None

    async def startup(self, app: web.Application):
        await self.shutdown(app)
        self._redis = await aioredis.create_redis_pool(REDIS_URL)

    async def shutdown(self, app: web.Application):
        if self._redis:
            self._redis.close()
            await self._redis.wait_closed()

    async def _mkeys(self, namespace: str, ids: Optional[List[str]], filter: Optional[str]) -> List[str]:
        keys = [keycat(namespace, key) for key in (ids or [])]
        if filter is not None:
            keys += [key.decode()
                     for key in await self._redis.keys(keycat(namespace, filter))]
        return keys

    async def ping(self) -> str:
        return (await self._redis.ping()).decode()

    async def get(self, namespace: str, id: str) -> dict:
        resp = await self._redis.get(keycat(namespace, id))
        return json.loads(resp) if resp else None

    async def mget(self, namespace: str, ids: List[str] = None, filter: str = None) -> List[dict]:
        keys = await self._mkeys(namespace, ids, filter)
        values = []
        if keys:
            values = await self._redis.mget(*keys)
        return [json.loads(v) for v in values]

    async def set(self, value: dict) -> dict:
        id = value['id']
        namespace = value['namespace']
        await self._redis.set(keycat(namespace, id), json.dumps(value))
        await mqtt.publish(self.app, TOPIC, {'changed': [value]})
        return value

    async def mset(self, values: List[dict]) -> List[dict]:
        if values:
            args = flatten([[keycat(v['namespace'], v['id']), json.dumps(v)] for v in values])
            await self._redis.mset(*args)
            await mqtt.publish(self.app, TOPIC, {'changed': values})
        return values

    async def delete(self, namespace: str, id: str) -> int:
        key = keycat(namespace, id)
        count = await self._redis.delete(key)
        await mqtt.publish(self.app, TOPIC, {'deleted': [key]})
        return count

    async def mdelete(self, namespace: str, ids: List[str] = None, filter: str = None) -> int:
        keys = await self._mkeys(namespace, ids, filter)
        count = 0
        if keys:
            count = await self._redis.delete(*keys)
            await mqtt.publish(self.app, TOPIC, {'deleted': keys})
        return count


def setup(app: web.Application):
    features.add(app, RedisClient(app))


def get_redis(app: web.Application) -> RedisClient:
    return features.get(app, RedisClient)
