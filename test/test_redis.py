"""
Tests brewblox_history.redis.py
"""

import json

import pytest
from brewblox_service.testing import response
from mock import AsyncMock, Mock, call

from brewblox_history import datastore_api, redis

TESTED = redis.__name__


@pytest.fixture
def m_redis(mocker):
    m = AsyncMock()
    m.close = Mock()  # not a coroutine
    mocker.patch(TESTED + '.aioredis.create_redis_pool', AsyncMock(return_value=m))
    return m


@pytest.fixture
def m_publish(mocker):
    m = mocker.patch(TESTED + '.mqtt.publish', AsyncMock())
    return m


@pytest.fixture
async def app(app, m_redis):
    redis.setup(app)
    datastore_api.setup(app)
    return app


@pytest.fixture
async def rclient(app, client):
    return redis.fget(app)


async def test_ping(m_redis, client, rclient: redis.RedisClient):
    m_redis.ping.return_value = b'pong'
    assert await rclient.ping() == 'pong'

    assert await response(client.get('/datastore/ping')) == {'ping': 'pong'}


async def test_get(m_redis, client, rclient: redis.RedisClient):
    m_redis.get.return_value = json.dumps({'hello': 'world'})
    assert await rclient.get('namespace', 'id') == {'hello': 'world'}
    m_redis.get.assert_awaited_with('namespace:id')

    await rclient.get('', 'id')
    m_redis.get.assert_awaited_with('id')

    assert await response(client.post('/datastore/get', json={
        'namespace': 'n',
        'id': 'x'
    })) == {
        'value': {'hello': 'world'}
    }
    # Missing namespace
    await response(client.post('/datastore/get', json={'id': 'x'}), 422)


async def test_get_none(m_redis, client, rclient: redis.RedisClient):
    m_redis.get.return_value = None
    assert await rclient.get('namespace', 'id') is None
    assert await response(client.post('/datastore/get', json={
        'namespace': 'n',
        'id': 'x'
    })) == {
        'value': None
    }


async def test_mget(m_redis, client, rclient: redis.RedisClient):
    m_redis.mget.side_effect = lambda *keys: [
        json.dumps({'idx': idx}) for idx in range(len(keys))
    ]
    m_redis.keys.return_value = [b'n1:k1', b'n2:k2']

    assert await rclient.mget('namespace') == [{'idx': 0}, {'idx': 1}]
    assert m_redis.mget.await_count == 1
    assert await rclient.mget('namespace', ['k']) == [{'idx': 0}]
    assert await rclient.mget('namespace', filter='*') == [{'idx': 0}, {'idx': 1}]
    m_redis.keys.assert_awaited_with('namespace:*')

    assert await response(client.post('/datastore/mget', json={
        'namespace': 'n',
        'ids': ['k'],
        'filter': '*',
    })) == {
        'values': [{'idx': 0}, {'idx': 1}, {'idx': 2}]
    }

    assert await response(client.post('/datastore/mget', json={
        'namespace': 'n',
    })) == {
        'values': [{'idx': 0}, {'idx': 1}]
    }
    m_redis.keys.assert_awaited_with('n:*')

    await response(client.post('/datastore/mget', json={}), 422)


async def test_mget_empty(m_redis, client, rclient: redis.RedisClient):
    m_redis.mget.side_effect = lambda *keys: [
        json.dumps({'idx': idx}) for idx in range(len(keys))
    ]
    m_redis.keys.return_value = []

    assert await response(client.post('/datastore/mget', json={
        'namespace': 'empty',
    })) == {
        'values': []
    }
    m_redis.keys.assert_awaited_with('empty:*')
    assert m_redis.mget.call_count == 0


async def test_set(app, m_redis, m_publish, client, rclient: redis.RedisClient):
    value = {'namespace': 'n:m', 'id': 'x', 'happy': True}
    assert await rclient.set(value) == value
    m_redis.set.assert_awaited_with('n:m:x', json.dumps(value))
    m_publish.assert_awaited_with(app, 'brewcast/datastore/n', {'changed': [value]}, err=False)

    assert await response(client.post('/datastore/set', json={
        'value': value
    })) == {
        'value': value
    }
    await response(client.post('/datastore/set', json={
        'value': {'id': 'x'},  # no namespace
    }), 422)


async def test_mset(app, m_redis, m_publish, client, rclient: redis.RedisClient):
    values = [{'namespace': 'n', 'id': 'x', 'happy': True}, {'namespace': 'n2', 'id': 'x2', 'jolly': False}]

    assert await rclient.mset([]) == []
    assert m_redis.mset.await_count == 0

    assert await rclient.mset(values) == values
    m_redis.mset.assert_awaited_with('n:x', json.dumps(values[0]), 'n2:x2', json.dumps(values[1]))
    m_publish.assert_has_awaits([
        call(app, 'brewcast/datastore/n', {'changed': [values[0]]}, err=False),
        call(app, 'brewcast/datastore/n2', {'changed': [values[1]]}, err=False),
    ], any_order=True)

    assert await response(client.post('/datastore/mset', json={
        'values': values
    })) == {
        'values': values
    }
    await response(client.post('/datastore/mset', json={
        'values': values + [{'id': 'y'}]
    }), 422)


async def test_delete(app, m_redis, m_publish, client, rclient: redis.RedisClient):
    m_redis.delete.return_value = 1
    assert await rclient.delete('n', 'x') == 1
    m_redis.delete.assert_awaited_with('n:x')
    m_publish.assert_awaited_with(app, 'brewcast/datastore/n', {'deleted': ['n:x']}, err=False)

    assert await response(client.post('/datastore/delete', json={
        'namespace': 'n',
        'id': 'x'
    })) == {
        'count': 1
    }
    await response(client.post('/datastore/delete', json={}), 422)


async def test_mdelete(app, m_redis, m_publish, client, rclient: redis.RedisClient):
    m_redis.keys.return_value = [b'n1:k1', b'n2:k2']
    m_redis.delete.side_effect = lambda *keys: len(keys)

    assert await rclient.mdelete('namespace') == 0
    assert m_publish.await_count == 0
    assert m_redis.delete.await_count == 0

    assert await rclient.mdelete('n', ['x', 'y:z']) == 2
    m_publish.assert_awaited_with(app, 'brewcast/datastore/n', {'deleted': ['n:x', 'n:y:z']}, err=False)

    assert await rclient.mdelete('n', ['x'], '*') == 3
    m_publish.assert_has_calls([
        call(app, 'brewcast/datastore/n', {'deleted': ['n:x']}, err=False),
        call(app, 'brewcast/datastore/n1', {'deleted': ['n1:k1']}, err=False),
        call(app, 'brewcast/datastore/n2', {'deleted': ['n2:k2']}, err=False),
    ], any_order=True)

    assert await response(client.post('/datastore/mdelete', json={
        'namespace': 'n',
    })) == {
        'count': 0
    }
    assert await response(client.post('/datastore/mdelete', json={
        'namespace': 'n',
        'filter': '*',
    })) == {
        'count': 2
    }
    await response(client.post('/datastore/mdelete', json={
        'filter': '*'
    }), 422)
