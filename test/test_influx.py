"""
Tests brewblox_history.influx
"""

import asyncio
from unittest.mock import Mock

import pytest
from aioresponses import aioresponses

from brewblox_history import influx

TESTED = influx.__name__


def allow_calls(response_mocker, url, count):
    for i in range(count):
        response_mocker.post(url + '/query', payload=dict(key='val'))
        response_mocker.get(url + '/ping')
        response_mocker.post(url + '/write')


@pytest.fixture
async def reduced_sleep(mocker):
    influx.WRITE_INTERVAL_S = 0.0001
    influx.RECONNECT_INTERVAL_S = 0.0001


@pytest.fixture
def influx_url():
    return 'http://localhost:8086'


@pytest.yield_fixture
async def mocked_influx(client, app, reduced_sleep, influx_url):
    with aioresponses() as res:
        allow_calls(res, influx_url, 5)
        yield res


@pytest.fixture
async def app(app, mocker):
    mocker.patch(TESTED + '.events.get_listener')

    influx.setup(app)
    return app


async def test_setup(app, client):
    assert influx.get_writer(app)
    assert influx.get_relay(app)
    assert influx.get_client(app)


async def test_endpoints_offline(app, client):
    assert (await client.post('/query', json={
        'database': 'brewblox',
        'query': 'select * from controller'
    })).status == 500

    assert (await client.post('/subscribe', json={
        'exchange': 'brewblox',
        'routing': 'controller.#'
    })).status == 200


async def test_runtime_construction(app, client):
    # App is running, objects should still be createable
    query_client = influx.QueryClient()
    await query_client.connect(app)

    writer = influx.InfluxWriter()
    await writer.start(app)

    influx.EventRelay(listener=Mock(), writer=writer)


async def test_query_client(app, client, mocked_influx):
    query_client = influx.get_client(app)
    data = await query_client.query(database='db', query='gimme')
    # corresponds to payload in 'mocked_influx'
    assert data['key'] == 'val'


async def test_running_writer(app, client, mocked_influx, mocker, influx_url):
    writer = influx.get_writer(app)
    # reconnect_spy = mocker.spy(writer, '_reconnect')

    await writer.write_soon(
        measurement='measurement',
        fields=dict(field1=1, field2=2),
        tags=dict(tag1=1, tag2=2)
    )

    allow_calls(mocked_influx, influx_url, 5)

    await asyncio.sleep(0.1)
    assert writer._task and not writer._task.done()
    assert not writer._pending
    # TODO(Bob): Mock the AsyncInfluxDBClient - aioresponses does not allow persistent
