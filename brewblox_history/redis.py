import json
import re
from functools import wraps
from itertools import groupby
from typing import List, Optional

import aioredis
from aiohttp import web
from brewblox_service import brewblox_logger, features, mqtt

LOGGER = brewblox_logger(__name__)

KEY_PATTERN = re.compile(r'^[\w\-\:]+$')


def keycat(namespace: str, key: str) -> str:
    return f'{namespace}:{key}' if namespace else key


def keycatobj(obj: dict) -> str:
    return keycat(obj['namespace'], obj['id'])


def keycheck(key: str) -> str:
    if not re.match(KEY_PATTERN, key):
        raise AssertionError(f'Invalid key: "{key}". Database keys can only contain letters, numbers, - or _')
    return key


def flatten(data: List):
    return [item for sublist in data for item in sublist]


def autoconnect(func):
    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        if not self._redis:
            self._redis = await aioredis.create_redis_pool(self.url)
        return await func(self, *args, **kwargs)
    return wrapper


class RedisClient(features.ServiceFeature):

    def __init__(self, app: web.Application):
        super().__init__(app)
        self.url = app['config']['redis_url']
        self.topic = app['config']['datastore_topic']
        # Lazy-loaded in autoconnect wrapper
        self._redis: aioredis.Redis = None

    async def startup(self, app: web.Application):
        await self.shutdown(app)

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

    async def _publish(self, changed: List[dict] = None, deleted: List[str] = None):
        """Publish changes to documents.

        Objects are grouped by top-level namespace, and then published
        to a topic postfixed with the top-level namespace.
        """
        if changed:
            changed = sorted(changed, key=keycatobj)
            for key, group in groupby(changed, key=lambda v: keycatobj(v).split(':')[0]):
                await mqtt.publish(self.app, f'{self.topic}/{key}', {'changed': list(group)}, err=False)

        if deleted:
            deleted = sorted(deleted)
            for key, group in groupby(deleted, key=lambda v: v.split(':')[0]):
                await mqtt.publish(self.app, f'{self.topic}/{key}', {'deleted': list(group)}, err=False)

    @autoconnect
    async def ping(self) -> str:
        return (await self._redis.ping()).decode()

    @autoconnect
    async def get(self, namespace: str, id: str) -> dict:
        resp = await self._redis.get(keycat(namespace, id))
        return json.loads(resp) if resp else None

    @autoconnect
    async def mget(self, namespace: str, ids: List[str] = None, filter: str = None) -> List[dict]:
        if ids is None and filter is None:
            filter = '*'
        keys = await self._mkeys(namespace, ids, filter)
        values = []
        if keys:
            values = await self._redis.mget(*keys)
        return [json.loads(v) for v in values]

    @autoconnect
    async def set(self, value: dict) -> dict:
        await self._redis.set(keycheck(keycatobj(value)), json.dumps(value))
        await self._publish(changed=[value])
        return value

    @autoconnect
    async def mset(self, values: List[dict]) -> List[dict]:
        if values:
            db_keys = [keycheck(keycatobj(v)) for v in values]
            db_values = [json.dumps(v) for v in values]
            await self._redis.mset(*flatten(zip(db_keys, db_values)))
            await self._publish(changed=values)
        return values

    @autoconnect
    async def delete(self, namespace: str, id: str) -> int:
        key = keycat(namespace, id)
        count = await self._redis.delete(key)
        await self._publish(deleted=[key])
        return count

    @autoconnect
    async def mdelete(self, namespace: str, ids: List[str] = None, filter: str = None) -> int:
        keys = await self._mkeys(namespace, ids, filter)
        count = 0
        if keys:
            count = await self._redis.delete(*keys)
            await self._publish(deleted=keys)
        return count


def setup(app: web.Application):
    features.add(app, RedisClient(app))


def fget(app: web.Application) -> RedisClient:
    return features.get(app, RedisClient)
