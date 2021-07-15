"""
Tests brewblox_history.timeseries_api
"""


import asyncio

import pytest
from aiohttp import ClientWebSocketResponse
from aiohttp.http_websocket import WSCloseCode
from brewblox_service.testing import response
from mock import AsyncMock

from brewblox_history import socket_closer, timeseries_api

TESTED = timeseries_api.__name__


@pytest.fixture
async def m_victoria(mocker):
    m = mocker.patch(TESTED + '.victoria.fget').return_value
    m.ping = AsyncMock()
    m.fields = AsyncMock()
    m.metrics = AsyncMock()
    m.ranges = AsyncMock()
    m.csv = AsyncMock()
    return m


@pytest.fixture
async def app(app):
    socket_closer.setup(app)
    timeseries_api.setup(app)
    return app


async def test_ping(app, client, m_victoria):
    await response(client.get('/timeseries/ping'))

    m_victoria.ping.side_effect = RuntimeError
    await response(client.get('/timeseries/ping'), status=500)


async def test_fields(app, client, m_victoria):
    m_victoria.fields.return_value = {'retv': True}
    assert await response(
        client.post('/timeseries/fields', json={'start': 'yesterday'})
    ) == {'retv': True}

    await response(
        client.post('/timeseries/fields', json={'start': 1}),
        status=422
    )


async def test_ranges(app, client, m_victoria):
    m_victoria.ranges.return_value = {'retv': True}
    assert await response(
        client.post('/timeseries/ranges', json={'fields': ['a', 'b', 'c']})
    ) == {'retv': True}

    await response(
        client.post('/timeseries/ranges', json={}),
        status=422
    )


async def test_metrics(app, client, m_victoria):
    m_victoria.metrics.return_value = {'retv': True}
    assert await response(
        client.post('/timeseries/metrics', json={'fields': ['a', 'b', 'c']})
    ) == {'retv': True}

    await response(
        client.post('/timeseries/metrics', json={}),
        status=422
    )


async def test_csv(app, client, m_victoria):
    async def csv_mock(fields, **kwargs):
        yield ','.join(fields)
        yield 'line 1'
        yield 'line 2'

    m_victoria.csv = csv_mock
    assert await response(
        client.post('/timeseries/csv', json={'fields': ['a', 'b', 'c'], 'precision': 's'})
    ) == 'a,b,c\nline 1\nline 2\n'

    await response(
        client.post('/timeseries/csv', json={}),
        status=422
    )


async def test_stream(app, client, m_victoria):
    app['config']['ranges_interval'] = 0.001
    m_victoria.metrics.return_value = {'metrics': True}
    m_victoria.ranges.return_value = {'ranges': True}

    async with client.ws_connect('/timeseries/stream') as ws:
        ws: ClientWebSocketResponse

        # Metrics
        await ws.send_json({
            'id': 'test-metrics',
            'command': 'metrics',
            'query': {
                'fields': ['a', 'b', 'c'],
            },
        })
        resp = await ws.receive_json(timeout=2)
        assert resp == {
            'id': 'test-metrics',
            'data': {
                'metrics': {'metrics': True},
            },
        }

        # Ranges
        await ws.send_json({
            'id': 'test-ranges-once',
            'command': 'ranges',
            'query': {
                'fields': ['a', 'b', 'c'],
                'end': '2021-07-15T14:29:30.000Z',
            },
        })
        resp = await ws.receive_json(timeout=2)
        assert resp == {
            'id': 'test-ranges-once',
            'data': {
                'initial': True,
                'ranges': {'ranges': True},
            },
        }
        with pytest.raises(asyncio.TimeoutError):
            await ws.receive_json(timeout=0.1)

        # Live ranges
        await ws.send_json({
            'id': 'test-ranges-live',
            'command': 'ranges',
            'query': {
                'fields': ['a', 'b', 'c'],
                'duration': '30m',
            },
        })
        resp = await ws.receive_json(timeout=2)
        assert resp == {
            'id': 'test-ranges-live',
            'data': {
                'initial': True,
                'ranges': {'ranges': True},
            },
        }
        resp = await ws.receive_json(timeout=2)
        assert resp == {
            'id': 'test-ranges-live',
            'data': {
                'initial': False,
                'ranges': {'ranges': True},
            },
        }

        # Stop live ranges
        await ws.send_json({
            'id': 'test-ranges-live',
            'command': 'stop',
        })


async def test_stream_error(app, client, m_victoria):
    m_victoria.ranges.side_effect = RuntimeError
    m_victoria.metrics.return_value = {'metrics': True}

    async with client.ws_connect('/timeseries/stream') as ws:
        ws: ClientWebSocketResponse

        # Backend raises error
        await ws.send_json({
            'id': 'test-ranges-once',
            'command': 'ranges',
            'query': {
                'fields': ['a', 'b', 'c'],
                'end': '2021-07-15T14:29:30.000Z',
            },
        })
        with pytest.raises(asyncio.TimeoutError):
            await ws.receive_json(timeout=0.1)

        # Other command is OK
        await ws.send_json({
            'id': 'test-metrics',
            'command': 'metrics',
            'query': {
                'fields': ['a', 'b', 'c'],
            },
        })
        resp = await ws.receive_json(timeout=2)
        assert resp == {
            'id': 'test-metrics',
            'data': {
                'metrics': {'metrics': True},
            },
        }


async def test_stream_invalid(app, client, m_victoria):
    async with client.ws_connect('/timeseries/stream') as ws:
        ws: ClientWebSocketResponse
        await ws.send_json({'empty': True})
        resp = await ws.receive_json(timeout=2)
        assert resp['error']


async def test_stream_close(app, client):
    async with client.ws_connect('/timeseries/stream') as ws:
        ws: ClientWebSocketResponse
        await socket_closer.fget(app).before_shutdown(app)
        resp = await ws.receive(timeout=1)
        assert resp.data == WSCloseCode.OK
