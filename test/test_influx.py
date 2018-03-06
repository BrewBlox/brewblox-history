"""
Tests brewblox_history.influx
"""

import asyncio
from unittest.mock import Mock, ANY

import pytest
from aiohttp.client_exceptions import ClientConnectionError
from asynctest import CoroutineMock

from brewblox_history import influx

TESTED = influx.__name__


class InfluxClientMock(Mock):

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


@pytest.fixture
async def reduced_sleep(mocker):
    influx.WRITE_INTERVAL_S = 0.0001
    influx.RECONNECT_INTERVAL_S = 0.0001


@pytest.fixture()
def mocked_influx(mocker):
    m = InfluxClientMock()
    [setattr(m.return_value, f, CoroutineMock()) for f in [
        'ping',
        'create_database',
        'write',
        'query',
        'close'
    ]]

    mocker.patch(TESTED + '.InfluxDBClient', m)
    return m


@pytest.fixture
async def app(app, mocker, mocked_influx, reduced_sleep):
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
    retval = dict(key='val')
    mocked_influx.return_value.query.return_value = retval

    data = await query_client.query(database='db', query='gimme')

    assert data == retval


async def test_running_writer(mocked_influx, app, client, mocker):
    writer = influx.get_writer(app)

    await writer.write_soon(
        measurement='measurement',
        fields=dict(field1=1, field2=2),
        tags=dict(tag1=1, tag2=2)
    )

    await asyncio.sleep(0.01)
    assert writer.is_running
    assert not writer._pending
    assert mocked_influx.return_value.write.call_count == 1


async def test_run_error(mocked_influx, app, client, mocker):
    writer = influx.get_writer(app)
    mocked_influx.return_value.ping.side_effect = RuntimeError
    warn_mock = mocker.spy(influx.LOGGER, 'warn')

    await asyncio.sleep(0.01)

    assert not writer.is_running
    assert warn_mock.call_count == 1
    await writer.close()
    assert warn_mock.call_count == 2


async def test_reconnect(mocked_influx, app, client):
    db_client = mocked_influx.return_value
    db_client.write.side_effect = [ClientConnectionError, RuntimeError]

    writer = influx.InfluxWriter()
    await writer.start(app)

    await writer.write_soon(
        measurement='measurement',
        fields=dict(field1=1, field2=2),
        tags=dict(tag1=1, tag2=2)
    )

    await asyncio.sleep(0.01)

    # the default writer connected just fine, and is idling
    # writing in first connection caused connection error
    # writing in second connection caused runtime error -> exit runner
    assert influx.get_writer(app).is_running
    assert not writer.is_running
    assert db_client.create_database.call_count == 3


async def test_reconnect_fail(mocked_influx, app, client):
    db_client = mocked_influx.return_value
    db_client.ping.side_effect = ClientConnectionError

    writer = influx.InfluxWriter()
    await writer.start(app)

    await asyncio.sleep(0.01)

    assert influx.get_writer(app).is_running
    assert writer.is_running
    # Only the background writer managed to connect before we introduced errors
    assert db_client.create_database.call_count == 1


async def test_relay_subscribe(mocked_influx, app, client):
    listener = influx.events.get_listener.return_value
    relay = influx.get_relay(app)
    relay.subscribe('arg', kw='kwarg')

    listener.subscribe.assert_called_once_with(
        'arg', kw='kwarg', on_message=relay._on_event_message)


async def test_relay_message(mocked_influx, app, client):
    db_client = mocked_influx.return_value
    relay = influx.get_relay(app)

    data = {
        'nest': {
            'ed': {
                'value': 'val'
            }
        }
    }

    flat_data = {
        'key/nest/ed/value': 'val'
    }

    flat_value = {
        'single/text': 'value'
    }

    await relay._on_event_message(None, 'route.key', data)
    await relay._on_event_message(None, 'route.single', 'value')

    expected = [
        {'time': ANY, 'measurement': 'route', 'fields': flat_data, 'tags': {}},
        {'time': ANY, 'measurement': 'route', 'fields': flat_value, 'tags': {}}
    ]

    await asyncio.sleep(0.1)
    db_client.write.assert_called_once_with(expected)
