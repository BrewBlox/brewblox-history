"""
Tests brewblox_history.redis.py
"""

import asyncio
from contextlib import AsyncExitStack
from unittest.mock import Mock, call

import pytest
from fastapi import FastAPI
from httpx import AsyncClient
from pytest_mock import MockerFixture

from brewblox_history import datastore_api, mqtt, redis
from brewblox_history.models import DatastoreValue

TESTED = redis.__name__


def sort_pyvalues(values: list[DatastoreValue]):
    return sorted(values, key=lambda v: v.id)


def sort_dictvalues(values: list[dict]):
    return sorted(values, key=lambda v: v['id'])


@pytest.fixture
def app():
    mqtt.setup()
    redis.setup()
    app = FastAPI()
    app.include_router(datastore_api.router)
    return app


@pytest.fixture
async def lifespan(app):
    async with AsyncExitStack() as stack:
        # Prevents test hangups if the connection fails
        async with asyncio.timeout(5):
            await stack.enter_async_context(mqtt.lifespan())
            await stack.enter_async_context(redis.lifespan())
            # Cleanup
            await redis.CV.get().mdelete('', filter='*')
        yield


@pytest.fixture
def m_publish(app, mocker: MockerFixture):
    m = mocker.spy(mqtt.CV.get(), 'publish')
    return m


async def test_ping(client: AsyncClient):
    await redis.CV.get().ping()

    resp = await client.get('/datastore/ping')
    assert resp.status_code == 200
    assert resp.json() == {'ping': 'pong'}


async def test_get(client: AsyncClient):
    c = redis.CV.get()
    obj = DatastoreValue(
        namespace='ns1',
        id='id1',
        hello='world',
    )
    assert await c.set(obj) == obj

    assert await c.get('ns1', 'id1') == obj
    assert await c.get('', 'id1') is None

    await c.set(obj)
    resp = await client.post('/datastore/get',
                             json={
                                 'namespace': 'ns1',
                                 'id': 'id1'
                             })
    assert resp.json() == {'value': obj.model_dump()}

    # Missing namespace
    resp = await client.post('/datastore/get',
                             json={'id': 'x'})
    assert resp.status_code == 422


async def test_get_none(client: AsyncClient):
    c = redis.CV.get()
    assert await c.get('namespace', 'id') is None
    resp = await client.post('/datastore/get',
                             json={
                                 'namespace': 'n',
                                 'id': 'x'
                             })
    assert resp.json() == {'value': None}


async def test_mget(client: AsyncClient):
    c = redis.CV.get()
    await c.mset([DatastoreValue(namespace='ns1', id=f'{idx}', idx=idx) for idx in range(2)])
    await c.mset([DatastoreValue(namespace='ns2', id=f'{idx}', idx=idx) for idx in range(3)])
    await c.mset([DatastoreValue(namespace='ns2', id=f'k{idx}', idx=idx) for idx in range(3)])

    assert sort_pyvalues(await c.mget('ns1')) == sort_pyvalues([
        DatastoreValue(namespace='ns1', id='0', idx=0),
        DatastoreValue(namespace='ns1', id='1', idx=1),
    ])
    assert sort_pyvalues(await c.mget('ns2', ['0'])) == sort_pyvalues([
        DatastoreValue(namespace='ns2', id='0', idx=0),
    ])
    assert sort_pyvalues(await c.mget('ns2', filter='*')) == sort_pyvalues([
        DatastoreValue(namespace='ns2', id='0', idx=0),
        DatastoreValue(namespace='ns2', id='1', idx=1),
        DatastoreValue(namespace='ns2', id='2', idx=2),
        DatastoreValue(namespace='ns2', id='k0', idx=0),
        DatastoreValue(namespace='ns2', id='k1', idx=1),
        DatastoreValue(namespace='ns2', id='k2', idx=2),
    ])

    resp = await client.post('/datastore/mget',
                             json={
                                 'namespace': 'ns2',
                                 'ids': ['2'],
                                 'filter': 'k*',
                             })
    assert sort_dictvalues(resp.json()['values']) == sort_dictvalues([
        {'namespace': 'ns2', 'id': '2', 'idx': 2},
        {'namespace': 'ns2', 'id': 'k0', 'idx': 0},
        {'namespace': 'ns2', 'id': 'k1', 'idx': 1},
        {'namespace': 'ns2', 'id': 'k2', 'idx': 2},
    ])

    resp = await client.post('/datastore/mget',
                             json={'namespace': 'n'})
    assert resp.json() == {'values': []}

    resp = await client.post('/datastore/mget',
                             json={})
    assert resp.status_code == 422


async def test_set(client: AsyncClient):
    value = DatastoreValue(namespace='n:m', id='x', happy=True)

    resp = await client.post('/datastore/set',
                             json={'value': value.model_dump()})
    assert resp.json() == {'value': value.model_dump()}

    resp = await client.post('/datastore/get',
                             json=value.model_dump())
    assert resp.json() == {'value': value.model_dump()}

    # no namespace in arg
    resp = await client.post('/datastore/set',
                             json={'value': {'id': 'x'}})
    assert resp.status_code == 422

    # invalid characters in id
    resp = await client.post('/datastore/set',
                             json={
                                 'value': {
                                     'namespace': 'n',
                                     'id': '[x]',
                                 },
                             })
    assert resp.status_code == 422


async def test_mset(client: AsyncClient, m_publish: Mock):
    c = redis.CV.get()
    values = [
        DatastoreValue(namespace='n', id='x', happy=True),
        DatastoreValue(namespace='n2', id='x2', jolly=False),
    ]
    dict_values = sort_dictvalues([v.model_dump() for v in values])

    assert await c.mset([]) == []
    assert await c.mset(values) == values

    m_publish.assert_has_calls([
        call('brewcast/datastore/n', {'changed': [dict_values[0]]}),
        call('brewcast/datastore/n2', {'changed': [dict_values[1]]}),
    ], any_order=True)

    resp = await client.post('/datastore/mset',
                             json={'values': dict_values})
    assert resp.json() == {'values': dict_values}

    resp = await client.post('/datastore/mset',
                             json={'values': dict_values + [{'id': 'y'}]})
    assert resp.status_code == 422


async def test_delete(client: AsyncClient, m_publish: Mock):
    c = redis.CV.get()
    await c.mset([
        DatastoreValue(namespace='ns1', id='id1'),
        DatastoreValue(namespace='ns1', id='id2'),
        DatastoreValue(namespace='ns2', id='id1'),
        DatastoreValue(namespace='ns2', id='id2'),
    ])

    assert await c.delete('ns1', 'id1') == 1

    m_publish.assert_called_with('brewcast/datastore/ns1',
                                 {'deleted': ['ns1:id1']})

    resp = await client.post('/datastore/delete',
                             json={
                                 'namespace': 'ns1',
                                 'id': 'id2',
                             })
    assert resp.json() == {'count': 1}

    resp = await client.post('/datastore/delete',
                             json={
                                 'namespace': 'ns1',
                                 'id': 'id2',
                             })
    assert resp.json() == {'count': 0}

    resp = await client.post('/datastore/delete',
                             json={})
    assert resp.status_code == 422


async def test_mdelete(client: AsyncClient, m_publish: Mock):
    c = redis.CV.get()
    await c.mset([
        DatastoreValue(namespace='ns1', id='id1'),
        DatastoreValue(namespace='ns1', id='id2'),
        DatastoreValue(namespace='ns2', id='id1'),
        DatastoreValue(namespace='ns2', id='id2'),
        DatastoreValue(namespace='ns3', id='id1'),
        DatastoreValue(namespace='ns3', id='id2'),
        DatastoreValue(namespace='ns3', id='id3'),
    ])

    m_publish.reset_mock()
    assert await c.mdelete('namespace') == 0
    assert m_publish.call_count == 0

    assert await c.mdelete('ns1', ['id1', 'id2', 'id3']) == 2
    m_publish.assert_called_with('brewcast/datastore/ns1',
                                 {'deleted': ['ns1:id1', 'ns1:id2', 'ns1:id3']})

    m_publish.reset_mock()
    assert await c.mdelete('', ['x'], '*2') == 2
    m_publish.assert_has_calls([
        call('brewcast/datastore/ns2',
             {'deleted': ['ns2:id2']}),
        call('brewcast/datastore/ns3',
             {'deleted': ['ns3:id2']}),
    ], any_order=True)

    resp = await client.post('/datastore/mdelete',
                             json={
                                 'namespace': 'n',
                             })
    assert resp.json() == {'count': 0}

    resp = await client.post('/datastore/mdelete',
                             json={
                                 'namespace': 'ns3',
                                 'filter': '*',
                             })
    assert resp.json() == {'count': 2}  # id1 and id3

    resp = await client.post('/datastore/mdelete',
                             json={
                                 'filter': '*'
                             })
    assert resp.status_code == 422
