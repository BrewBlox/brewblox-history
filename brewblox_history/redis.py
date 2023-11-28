# import json
import logging
from contextlib import asynccontextmanager
from contextvars import ContextVar
from functools import wraps
from itertools import groupby

from redis import asyncio as aioredis

from . import mqtt
from .models import DatastoreValue, ServiceConfig

LOGGER = logging.getLogger(__name__)

CV: ContextVar['RedisClient'] = ContextVar('RedisClient')


def keycat(namespace: str, key: str) -> str:
    return f'{namespace}:{key}' if namespace else key


def keycatobj(obj: DatastoreValue) -> str:
    return keycat(obj.namespace, obj.id)


def autoconnect(func):
    @wraps(func)
    async def wrapper(self: 'RedisClient', *args, **kwargs):
        if not self._redis:
            self._redis = await aioredis.from_url(self.url)
            await self._redis
        return await func(self, *args, **kwargs)
    return wrapper


class RedisClient:

    def __init__(self):
        config = ServiceConfig.cached()
        self.url = config.redis_url
        self.topic = config.datastore_topic
        # Lazy-loaded in autoconnect wrapper
        self._redis: aioredis.Redis = None

    async def disconnect(self):
        if self._redis:
            await self._redis.close()

    async def _mkeys(self, namespace: str, ids: list[str] | None, filter: str | None) -> list[str]:
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
        fast_mqtt = mqtt.CV.get()

        if changed:
            changed = sorted(changed, key=keycatobj)
            for key, group in groupby(changed, key=lambda v: keycatobj(v).split(':')[0]):
                fast_mqtt.publish(f'{self.topic}/{key}',
                                  {'changed': list((v.model_dump() for v in group))})

        if deleted:
            deleted = sorted(deleted)
            for key, group in groupby(deleted, key=lambda v: v.split(':')[0]):
                fast_mqtt.publish(f'{self.topic}/{key}',
                                  {'deleted': list(group)})

    @autoconnect
    async def ping(self):
        await self._redis.ping()

    @autoconnect
    async def get(self, namespace: str, id: str) -> DatastoreValue | None:
        resp = await self._redis.get(keycat(namespace, id))
        return DatastoreValue.model_validate_json(resp) if resp else None

    @autoconnect
    async def mget(self, namespace: str, ids: list[str] = None, filter: str = None) -> list[DatastoreValue]:
        if ids is None and filter is None:
            filter = '*'
        keys = await self._mkeys(namespace, ids, filter)
        values = []
        if keys:
            values = await self._redis.mget(*keys)
        return [DatastoreValue.model_validate_json(v)
                for v in values
                if v is not None]

    @autoconnect
    async def set(self, value: DatastoreValue) -> DatastoreValue:
        await self._redis.set(keycatobj(value), value.model_dump_json())
        await self._publish(changed=[value])
        return value

    @autoconnect
    async def mset(self, values: list[DatastoreValue]) -> list[DatastoreValue]:
        if values:
            db_keys = [keycatobj(v) for v in values]
            db_values = [v.model_dump_json() for v in values]
            await self._redis.mset(dict(zip(db_keys, db_values)))
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


def setup():
    CV.set(RedisClient())


@asynccontextmanager
async def lifespan():
    yield
    await CV.get().disconnect()
