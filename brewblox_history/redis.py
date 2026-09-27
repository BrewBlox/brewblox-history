import logging
from collections.abc import AsyncIterator
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
    def __init__(self) -> None:
        config = utils.get_config()
        self.url = f'redis://{config.redis_host}:{config.redis_port}'
        self.topic = config.datastore_topic
        self._redis: aioredis.Redis | None = None

    async def connect(self) -> None:
        await self.disconnect()
        self._redis = await aioredis.from_url(self.url)
        await self._db.initialize()

    @property
    def _db(self) -> aioredis.Redis:
        if self._redis is None:
            raise ConnectionError('Not connected to Redis')
        return self._redis

    async def disconnect(self) -> None:
        if self._redis:
            await self._redis.aclose()
            self._redis = None

    async def _mkeys(self, namespace: str, ids: list[str] | None, pattern: str | None) -> list[str]:
        keys = [keycat(namespace, key) for key in (ids or [])]
        if pattern is not None:
            # The client returns bytes (no decode_responses), whatever its types say
            found = await self._db.keys(keycat(namespace, pattern))
            keys += [key.decode() if isinstance(key, bytes) else key for key in found]
        return keys

    async def _publish(self, changed: list[DatastoreValue] | None = None, deleted: list[str] | None = None) -> None:
        """Publish changes to documents.

        Objects are grouped by top-level namespace, and then published
        to a topic postfixed with the top-level namespace.
        """
        fmqtt = mqtt.CV.get()

        if changed:
            changed = sorted(changed, key=keycatobj)
            for key, group in groupby(changed, key=lambda v: keycatobj(v).split(':')[0]):
                fmqtt.publish(f'{self.topic}/{key}', {'changed': [v.model_dump() for v in group]})

        if deleted:
            deleted = sorted(deleted)
            for key, group in groupby(deleted, key=lambda v: v.split(':')[0]):
                fmqtt.publish(f'{self.topic}/{key}', {'deleted': list(group)})

    async def ping(self) -> None:
        await self._db.ping()

    async def get(self, namespace: str, doc_id: str) -> DatastoreValue | None:
        resp = await self._db.get(keycat(namespace, doc_id))
        return DatastoreValue.model_validate_json(resp) if resp else None

    async def mget(
        self,
        namespace: str,
        ids: list[str] | None = None,
        pattern: str | None = None,
    ) -> list[DatastoreValue]:
        if ids is None and pattern is None:
            pattern = '*'
        keys = await self._mkeys(namespace, ids, pattern)
        values = []
        if keys:
            values = await self._db.mget(*keys)
        return [DatastoreValue.model_validate_json(v) for v in values if v is not None]

    async def set(self, value: DatastoreValue) -> DatastoreValue:
        await self._db.set(keycatobj(value), value.model_dump_json())
        await self._publish(changed=[value])
        return value

    async def mset(self, values: list[DatastoreValue]) -> list[DatastoreValue]:
        if values:
            db_keys = [keycatobj(v) for v in values]
            db_values = [v.model_dump_json() for v in values]
            await self._db.mset(dict(zip(db_keys, db_values, strict=True)))
            await self._publish(changed=values)
        return values

    async def delete(self, namespace: str, doc_id: str) -> int:
        key = keycat(namespace, doc_id)
        count = await self._db.delete(key)
        await self._publish(deleted=[key])
        return count

    async def mdelete(self, namespace: str, ids: list[str] | None = None, pattern: str | None = None) -> int:
        keys = await self._mkeys(namespace, ids, pattern)
        count = 0
        if keys:
            count = await self._db.delete(*keys)
            await self._publish(deleted=keys)
        return count


def setup() -> None:
    CV.set(RedisClient())


@asynccontextmanager
async def lifespan() -> AsyncIterator[None]:
    client = CV.get()
    await client.connect()
    yield
    await client.disconnect()
