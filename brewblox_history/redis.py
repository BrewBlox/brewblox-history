from functools import wraps
from itertools import groupby
from typing import Optional

import aioredis
from aiohttp import web
from brewblox_service import brewblox_logger, features, mqtt

from brewblox_history.models import DatastoreValue

LOGGER = brewblox_logger(__name__)


def keycat(namespace: str, key: str) -> str:
    return f'{namespace}:{key}' if namespace else key


def keycatobj(obj: DatastoreValue) -> str:
    return keycat(obj.namespace, obj.id)


def flatten(data: list):
    return [item for sublist in data for item in sublist]


def autoconnect(func):
    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        if not self._redis:
            self._redis = await aioredis.from_url(self.url)
        return await func(self, *args, **kwargs)
    return wrapper


class RedisClient(features.ServiceFeature):

    def __init__(self, app: web.Application):
        super().__init__(app)
        self.url = app['config']['redis_url']
        self.topic = app['config']['datastore_topic']
        # Lazy-loaded in autoconnect wrapper
        self._redis: aioredis.Redis = None

    async def shutdown(self, app: web.Application):
        if self._redis:
            await self._redis.close()

    async def _mkeys(self, namespace: str, ids: Optional[list[str]], filter: Optional[str]) -> list[str]:
        keys = [keycat(namespace, key) for key in (ids or [])]
        if filter is not None:
            keys += [key.decode()
                     for key in await self._redis.keys(keycat(namespace, filter))]
        return keys

    async def _publish(self, changed: list[DatastoreValue] = None, deleted: list[str] = None):
        """Publish changes to documents.

        Objects are grouped by top-level namespace, and then published
        to a topic postfixed with the top-level namespace.
        """
        if changed:
            changed = sorted(changed, key=keycatobj)
            for key, group in groupby(changed, key=lambda v: keycatobj(v).split(':')[0]):
                await mqtt.publish(self.app,
                                   f'{self.topic}/{key}',
                                   {'changed': list((v.dict() for v in group))},
                                   err=False)

        if deleted:
            deleted = sorted(deleted)
            for key, group in groupby(deleted, key=lambda v: v.split(':')[0]):
                await mqtt.publish(self.app,
                                   f'{self.topic}/{key}',
                                   {'deleted': list(group)},
                                   err=False)

    @autoconnect
    async def ping(self):
        await self._redis.ping()

    @autoconnect
    async def get(self, namespace: str, id: str) -> Optional[DatastoreValue]:
        resp = await self._redis.get(keycat(namespace, id))
        return DatastoreValue.parse_raw(resp) if resp else None

    @autoconnect
    async def mget(self, namespace: str, ids: list[str] = None, filter: str = None) -> list[DatastoreValue]:
        if ids is None and filter is None:
            filter = '*'
        keys = await self._mkeys(namespace, ids, filter)
        values = []
        if keys:
            values = await self._redis.mget(*keys)
        return [DatastoreValue.parse_raw(v)
                for v in values
                if v is not None]

    @autoconnect
    async def set(self, value: DatastoreValue) -> DatastoreValue:
        await self._redis.set(keycatobj(value), value.json())
        await self._publish(changed=[value])
        return value

    @autoconnect
    async def mset(self, values: list[DatastoreValue]) -> list[DatastoreValue]:
        if values:
            db_keys = [keycatobj(v) for v in values]
            db_values = [v.json() for v in values]
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
    async def mdelete(self, namespace: str, ids: list[str] = None, filter: str = None) -> int:
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
