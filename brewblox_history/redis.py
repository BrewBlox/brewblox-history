# import json
import logging
from contextlib import asynccontextmanager
from contextvars import ContextVar
from itertools import groupby

from redis import asyncio as aioredis

from . import mqtt, utils
from .models import DatastoreValue

LOGGER = logging.getLogger(__name__)

CV: ContextVar['RedisClient'] = ContextVar('redis.client')


def keycat(namespace: str, key: str) -> str:
    return f'{namespace}:{key}' if namespace else key


def keycatobj(obj: DatastoreValue) -> str:
    return keycat(obj.namespace, obj.id)


class RedisClient:

    def __init__(self):
        config = utils.get_config()
        self.url = f'redis://{config.redis_host}:{config.redis_port}'
        self.topic = config.datastore_topic
        self._redis: aioredis.Redis = None

    async def connect(self):
        await self.disconnect()
        self._redis = await aioredis.from_url(self.url)
        await self._redis.initialize()

    async def disconnect(self):
        if self._redis:
            await self._redis.aclose()
            self._redis = None

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
        fmqtt = mqtt.CV.get()

        if changed:
            changed = sorted(changed, key=keycatobj)
            for key, group in groupby(changed, key=lambda v: keycatobj(v).split(':')[0]):
                fmqtt.publish(f'{self.topic}/{key}',
                              {'changed': list((v.model_dump() for v in group))})

        if deleted:
            deleted = sorted(deleted)
            for key, group in groupby(deleted, key=lambda v: v.split(':')[0]):
                fmqtt.publish(f'{self.topic}/{key}',
                              {'deleted': list(group)})

    async def ping(self):
        await self._redis.ping()

    async def get(self, namespace: str, id: str) -> DatastoreValue | None:
        resp = await self._redis.get(keycat(namespace, id))
        return DatastoreValue.model_validate_json(resp) if resp else None

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

    async def set(self, value: DatastoreValue) -> DatastoreValue:
        await self._redis.set(keycatobj(value), value.model_dump_json())
        await self._publish(changed=[value])
        return value

    async def mset(self, values: list[DatastoreValue]) -> list[DatastoreValue]:
        if values:
            db_keys = [keycatobj(v) for v in values]
            db_values = [v.model_dump_json() for v in values]
            await self._redis.mset(dict(zip(db_keys, db_values)))
            await self._publish(changed=values)
        return values

    async def delete(self, namespace: str, id: str) -> int:
        key = keycat(namespace, id)
        count = await self._redis.delete(key)
        await self._publish(deleted=[key])
        return count

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
    client = CV.get()
    await client.connect()
    yield
    await client.disconnect()
