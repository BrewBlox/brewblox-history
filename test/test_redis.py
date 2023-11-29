"""
Tests brewblox_history.redis.py
"""

import asyncio
import json
from typing import Generator
from unittest.mock import call

import pytest
from httpx import AsyncClient
from pytest_mock import MockerFixture

from brewblox_history import app, datastore_api, mqtt, redis
from brewblox_history.models import DatastoreValue

TESTED = redis.__name__
pytestmark = pytest.mark.anyio


def sort_pyvalues(values: list[DatastoreValue]):
    return sorted(values, key=lambda v: v.id)


def sort_dictvalues(values: list[dict]):
    return sorted(values, key=lambda v: v['id'])


@pytest.fixture
async def client():
    async with AsyncClient(app=app.create_app(),
                           base_url='http://test',
                           ) as ac:
        yield ac


@pytest.fixture
def m_publish(client, mocker: MockerFixture):
    m = mocker.spy(mqtt.CV.get(), 'publish')
    return m


@pytest.fixture(autouse=True)
async def synchronized(client: AsyncClient):
    # Prevents test hangups if the connection fails
    async with asyncio.timeout(5):
        asyncio.gather(
            redis.CV.get().ping(),
            mqtt.CV_CONNECTED.get().wait())
    await redis.CV.get().mdelete('', filter='*')


async def test_ping(client: AsyncClient):
    await redis.CV.get().ping()
    assert (await client.get('/history/datastore/ping')).json() == {'ping': 'pong'}


# async def test_get(app, client):
#     c = redis.fget(app)
#     obj = DatastoreValue(
#         namespace='ns1',
#         id='id1',
#         hello='world',
#     )
#     assert await c.set(obj) == obj

#     assert await c.get('ns1', 'id1') == obj
#     assert await c.get('', 'id1') is None

#     await c.set(obj)
#     assert await response(client.post('/datastore/get', json={
#         'namespace': 'ns1',
#         'id': 'id1'
#     })) == {'value': obj.dict()}

#     # Missing namespace
#     await response(client.post('/datastore/get', json={'id': 'x'}), 400)


# async def test_get_none(app, client):
#     c = redis.fget(app)
#     assert await c.get('namespace', 'id') is None
#     assert await response(client.post('/datastore/get', json={
#         'namespace': 'n',
#         'id': 'x'
#     })) == {
#         'value': None
#     }


# async def test_mget(app, client):
#     c = redis.fget(app)
#     await c.mset([DatastoreValue(namespace='ns1', id=f'{idx}', idx=idx) for idx in range(2)])
#     await c.mset([DatastoreValue(namespace='ns2', id=f'{idx}', idx=idx) for idx in range(3)])
#     await c.mset([DatastoreValue(namespace='ns2', id=f'k{idx}', idx=idx) for idx in range(3)])

#     assert sort_pyvalues(await c.mget('ns1')) == sort_pyvalues([
#         DatastoreValue(namespace='ns1', id='0', idx=0),
#         DatastoreValue(namespace='ns1', id='1', idx=1),
#     ])
#     assert sort_pyvalues(await c.mget('ns2', ['0'])) == sort_pyvalues([
#         DatastoreValue(namespace='ns2', id='0', idx=0),
#     ])
#     assert sort_pyvalues(await c.mget('ns2', filter='*')) == sort_pyvalues([
#         DatastoreValue(namespace='ns2', id='0', idx=0),
#         DatastoreValue(namespace='ns2', id='1', idx=1),
#         DatastoreValue(namespace='ns2', id='2', idx=2),
#         DatastoreValue(namespace='ns2', id='k0', idx=0),
#         DatastoreValue(namespace='ns2', id='k1', idx=1),
#         DatastoreValue(namespace='ns2', id='k2', idx=2),
#     ])

#     resp = await response(client.post('/datastore/mget', json={
#         'namespace': 'ns2',
#         'ids': ['2'],
#         'filter': 'k*',
#     }))
#     assert sort_dictvalues(resp['values']) == sort_dictvalues([
#         {'namespace': 'ns2', 'id': '2', 'idx': 2},
#         {'namespace': 'ns2', 'id': 'k0', 'idx': 0},
#         {'namespace': 'ns2', 'id': 'k1', 'idx': 1},
#         {'namespace': 'ns2', 'id': 'k2', 'idx': 2},
#     ])

#     assert await response(client.post('/datastore/mget', json={
#         'namespace': 'n',
#     })) == {'values': []}

#     await response(client.post('/datastore/mget', json={}), 400)


# async def test_set(app,  m_publish, client):
#     value = DatastoreValue(namespace='n:m', id='x', happy=True)

#     assert await response(client.post('/datastore/set', json={
#         'value': value.dict()
#     })) == {
#         'value': value.dict()
#     }
#     assert await response(client.post('/datastore/get', json=value.dict())) == {
#         'value': value.dict()
#     }

#     # no namespace in arg
#     await response(client.post('/datastore/set', json={
#         'value': {'id': 'x'},
#     }), 400)

#     # invalid characters in id
#     await response(client.post('/datastore/set', json={
#         'value': {'namespace': 'n', 'id': '[x]'},
#     }), 400)


# async def test_mset(app, m_publish, client):
#     c = redis.fget(app)
#     values = [
#         DatastoreValue(namespace='n', id='x', happy=True),
#         DatastoreValue(namespace='n2', id='x2', jolly=False),
#     ]
#     dict_values = sort_dictvalues([v.dict() for v in values])

#     assert await c.mset([]) == []
#     assert await c.mset(values) == values

#     m_publish.assert_has_awaits([
#         call(app, 'brewcast/datastore/n', json.dumps({'changed': [dict_values[0]]}), err=False),
#         call(app, 'brewcast/datastore/n2', json.dumps({'changed': [dict_values[1]]}), err=False),
#     ], any_order=True)

#     assert await response(client.post('/datastore/mset', json={
#         'values': dict_values
#     })) == {
#         'values': dict_values
#     }
#     await response(client.post('/datastore/mset', json={
#         'values': dict_values + [{'id': 'y'}]
#     }), 400)


# async def test_delete(app, m_publish, client):
#     c = redis.fget(app)
#     await c.mset([
#         DatastoreValue(namespace='ns1', id='id1'),
#         DatastoreValue(namespace='ns1', id='id2'),
#         DatastoreValue(namespace='ns2', id='id1'),
#         DatastoreValue(namespace='ns2', id='id2'),
#     ])

#     assert await c.delete('ns1', 'id1') == 1

#     m_publish.assert_awaited_with(app,
#                                   topic='brewcast/datastore/ns1',
#                                   payload=json.dumps({'deleted': ['ns1:id1']}),
#                                   err=False)

#     assert await response(client.post('/datastore/delete', json={
#         'namespace': 'ns1',
#         'id': 'id2'
#     })) == {
#         'count': 1
#     }
#     assert await response(client.post('/datastore/delete', json={
#         'namespace': 'ns1',
#         'id': 'id2'
#     })) == {
#         'count': 0
#     }
#     await response(client.post('/datastore/delete', json={}), 400)


# async def test_mdelete(app, m_publish, client):
#     c = redis.fget(app)
#     await c.mset([
#         DatastoreValue(namespace='ns1', id='id1'),
#         DatastoreValue(namespace='ns1', id='id2'),
#         DatastoreValue(namespace='ns2', id='id1'),
#         DatastoreValue(namespace='ns2', id='id2'),
#         DatastoreValue(namespace='ns3', id='id1'),
#         DatastoreValue(namespace='ns3', id='id2'),
#         DatastoreValue(namespace='ns3', id='id3'),
#     ])

#     m_publish.reset_mock()
#     assert await c.mdelete('namespace') == 0
#     assert m_publish.await_count == 0

#     assert await c.mdelete('ns1', ['id1', 'id2', 'id3']) == 2
#     m_publish.assert_awaited_with(app,
#                                   topic='brewcast/datastore/ns1',
#                                   payload=json.dumps({'deleted': ['ns1:id1', 'ns1:id2', 'ns1:id3']}),
#                                   err=False)

#     m_publish.reset_mock()
#     assert await c.mdelete('', ['x'], '*2') == 2
#     m_publish.assert_has_calls([
#         call(app,
#              topic='brewcast/datastore/ns2',
#              payload=json.dumps({'deleted': ['ns2:id2']}),
#              err=False),
#         call(app,
#              topic='brewcast/datastore/ns3',
#              payload=json.dumps({'deleted': ['ns3:id2']}),
#              err=False),
#     ], any_order=True)

#     assert await response(client.post('/datastore/mdelete', json={
#         'namespace': 'n',
#     })) == {
#         'count': 0
#     }
#     assert await response(client.post('/datastore/mdelete', json={
#         'namespace': 'ns3',
#         'filter': '*',
#     })) == {
#         'count': 2  # id1 and id3
#     }
#     await response(client.post('/datastore/mdelete', json={
#         'filter': '*'
#     }), 400)
