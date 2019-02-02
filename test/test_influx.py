"""
Tests brewblox_history.influx
"""

import asyncio
from unittest.mock import Mock

import pytest
from aiohttp.client_exceptions import ClientConnectionError
from asynctest import CoroutineMock
from brewblox_service import scheduler

from brewblox_history import influx

TESTED = influx.__name__


class InfluxClientMock(Mock):

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


@pytest.fixture
async def reduced_sleep(mocker):
    mocker.patch.object(influx, 'RECONNECT_INTERVAL_S', 0.001)


@pytest.fixture
async def fewer_max_points(mocker):
    mocker.patch.object(influx, 'MAX_PENDING_POINTS', 10)


@pytest.fixture()
def influx_mock(mocker):
    m = InfluxClientMock()
    [setattr(m.return_value, f, CoroutineMock()) for f in [
        'ping',
        'create_database',
        'write',
        'query',
        'close'
    ]]

    mocker.patch(TESTED + '.InfluxDBClient', m)
    return m.return_value


@pytest.fixture
async def app(app, mocker, influx_mock, reduced_sleep):
    app['config']['write_interval'] = 0.001
    scheduler.setup(app)
    influx.setup(app)
    return app


async def test_setup(app, client):
    assert influx.get_data_writer(app)
    assert influx.get_client(app)


async def test_runtime_construction(app, client):
    # App is running, objects should still be createable
    query_client = influx.QueryClient(app)
    await query_client.startup(app)
    await query_client.shutdown()
    await query_client.shutdown()

    writer = influx.InfluxWriter(app)
    await writer.startup(app)
    assert writer.is_running
    await writer.shutdown()
    await writer.shutdown()


async def test_query_client(app, client, influx_mock):
    query_client = influx.get_client(app)
    retval = dict(key='val')
    influx_mock.query.return_value = retval

    data = await query_client.query(database='db', query='gimme')

    assert data == retval


async def test_running_writer(influx_mock, app, client, mocker):
    writer = influx.get_data_writer(app)

    await writer.write_soon(
        measurement='measurement',
        fields=dict(field1=1, field2=2),
        tags=dict(tag1=1, tag2=2)
    )

    await asyncio.sleep(0.1)
    assert writer.is_running
    assert not writer._pending
    assert influx_mock.write.call_count == 1


async def test_run_error(influx_mock, app, client, mocker):
    data_writer = influx.get_data_writer(app)
    influx_mock.ping.side_effect = RuntimeError

    with pytest.warns(UserWarning, match='RuntimeError'):
        await asyncio.sleep(0.1)

    assert not data_writer.is_running


async def test_retry_generate_connection(influx_mock, app, client):
    writer = influx.get_data_writer(app)
    await writer.shutdown()

    influx_mock.ping.reset_mock()
    influx_mock.create_database.reset_mock()

    influx_mock.ping.side_effect = ClientConnectionError

    await writer.startup(app)
    await asyncio.sleep(0.1)

    # generate_connections() keeps trying, but no success so far
    assert writer.is_running
    assert influx_mock.ping.call_count > 0
    assert influx_mock.create_database.call_count == 0


async def test_reconnect(influx_mock, app, client):
    writer = influx.get_data_writer(app)

    influx_mock.create_database.side_effect = ClientConnectionError

    await writer.shutdown()
    await writer.startup(app)

    await writer.write_soon(
        measurement='measurement',
        fields=dict(field1=1, field2=2),
        tags=dict(tag1=1, tag2=2)
    )

    await asyncio.sleep(0.1)
    assert influx_mock.write.call_count == 0
    assert writer.is_running

    influx_mock.create_database.side_effect = [True]

    await asyncio.sleep(0.1)
    assert writer.is_running
    assert influx_mock.write.call_count == 1


async def test_downsample(influx_mock, app, client, fewer_max_points):
    influx_mock.create_database.side_effect = ClientConnectionError

    writer = influx.get_data_writer(app)
    await writer.shutdown()
    await writer.startup(app)

    for i in range(2 * influx.MAX_PENDING_POINTS + 1):
        await writer.write_soon(
            measurement='measurement',
            fields=dict(field1=1, field2=2),
            tags=dict(tag1=1, tag2=2)
        )

    # It's been downsampled to half every time it hit max points
    assert len(writer._pending) == 0.5 * influx.MAX_PENDING_POINTS + 1
    assert influx_mock.write.call_count == 0
    influx_mock.create_database.side_effect = None
    await asyncio.sleep(0.1)
