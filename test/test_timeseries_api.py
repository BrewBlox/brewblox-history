"""
Tests brewblox_history.timeseries_api
"""


import asyncio
from datetime import datetime, timezone
from time import time_ns
from unittest.mock import ANY, AsyncMock

import pytest
from aiohttp import ClientWebSocketResponse
from aiohttp.http_websocket import WSCloseCode
from brewblox_service.testing import response
from pytest import approx

from brewblox_history import socket_closer, timeseries_api
from brewblox_history.models import (TimeSeriesCsvQuery, TimeSeriesMetric,
                                     TimeSeriesRange, TimeSeriesRangeMetric,
                                     TimeSeriesRangeValue)

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
    m_victoria.fields.return_value = ['a', 'b', 'c']
    assert await response(
        client.post('/timeseries/fields', json={'duration': '1d'})
    ) == ['a', 'b', 'c']

    await response(
        client.post('/timeseries/fields', json={'duration': []}),
        status=400,
    )


async def test_ranges(app, client, m_victoria):
    m_victoria.ranges.return_value = [
        TimeSeriesRange(
            metric=TimeSeriesRangeMetric(__name__='a'),
            values=[TimeSeriesRangeValue(1234, '54321')]
        ),
        TimeSeriesRange(
            metric=TimeSeriesRangeMetric(__name__='b'),
            values=[TimeSeriesRangeValue(2345, '54321')]
        ),
        TimeSeriesRange(
            metric=TimeSeriesRangeMetric(__name__='c'),
            values=[TimeSeriesRangeValue(3456, '54321')]
        ),
    ]
    assert await response(
        client.post('/timeseries/ranges', json={'fields': ['a', 'b', 'c']})
    ) == [
        {
            'metric': {'__name__': 'a'},
            'values': [[1234, '54321']],
        },
        {
            'metric': {'__name__': 'b'},
            'values': [[2345, '54321']],
        },
        {
            'metric': {'__name__': 'c'},
            'values': [[3456, '54321']],
        },
    ]

    await response(
        client.post('/timeseries/ranges', json={}),
        status=400
    )


async def test_metrics(app, client, m_victoria):
    now = time_ns() // 1_000_000
    m_victoria.metrics.return_value = [
        TimeSeriesMetric(
            metric='a',
            value=1.2,
            timestamp=now
        ),
        TimeSeriesMetric(
            metric='b',
            value=2.2,
            timestamp=now
        ),
        TimeSeriesMetric(
            metric='c',
            value=3.2,
            timestamp=now
        ),
    ]
    assert await response(
        client.post('/timeseries/metrics', json={'fields': ['a', 'b', 'c']})
    ) == [
        {'metric': 'a', 'value': approx(1.2), 'timestamp': now},
        {'metric': 'b', 'value': approx(2.2), 'timestamp': now},
        {'metric': 'c', 'value': approx(3.2), 'timestamp': now},
    ]

    await response(
        client.post('/timeseries/metrics', json={}),
        status=400
    )


async def test_csv(app, client, m_victoria):
    async def csv_mock(args: TimeSeriesCsvQuery):
        yield ','.join(args.fields)
        yield 'line 1'
        yield 'line 2'

    m_victoria.csv = csv_mock
    assert await response(
        client.post('/timeseries/csv', json={'fields': ['a', 'b', 'c'], 'precision': 's'})
    ) == 'a,b,c\nline 1\nline 2\n'

    await response(
        client.post('/timeseries/csv', json={}),
        status=400
    )


async def test_empty_csv(app, client, m_victoria):
    async def csv_mock(args: TimeSeriesCsvQuery):
        yield ','.join(args.fields)

    m_victoria.csv = csv_mock
    assert await response(
        client.post('/timeseries/csv', json={'fields': ['a', 'b', 'c'], 'precision': 's'})
    ) == 'a,b,c\n'


async def test_stream(app, client, m_victoria):
    app['config']['ranges_interval'] = 0.001
    m_victoria.metrics.return_value = [
        TimeSeriesMetric(
            metric='a',
            value=1.2,
            timestamp=1
        ),
        TimeSeriesMetric(
            metric='b',
            value=2.2,
            timestamp=1
        ),
        TimeSeriesMetric(
            metric='c',
            value=3.2,
            timestamp=1
        ),
    ]
    m_victoria.ranges.return_value = [
        TimeSeriesRange(
            metric={'__name__': 'a'},
            values=[TimeSeriesRangeValue(1234, '54321')]
        ),
        TimeSeriesRange(
            metric={'__name__': 'b'},
            values=[TimeSeriesRangeValue(2345, '54321')]
        ),
        TimeSeriesRange(
            metric={'__name__': 'c'},
            values=[TimeSeriesRangeValue(3456, '54321')]
        ),
    ]

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
                'metrics': [ANY, ANY, ANY],
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
                'ranges': [ANY, ANY, ANY],
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
                'ranges': [ANY, ANY, ANY],
            },
        }
        resp = await ws.receive_json(timeout=2)
        assert resp == {
            'id': 'test-ranges-live',
            'data': {
                'initial': False,
                'ranges': [ANY, ANY, ANY],
            },
        }

        # Stop live ranges
        await ws.send_json({
            'id': 'test-ranges-live',
            'command': 'stop',
        })


async def test_stream_error(app, client, m_victoria):
    m_victoria.ranges.side_effect = RuntimeError
    m_victoria.metrics.return_value = [
        TimeSeriesMetric(
            metric='a',
            value=1.2,
            timestamp=datetime(2021, 7, 15, 19, tzinfo=timezone.utc)
        ),
    ]

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
                'metrics': [{
                    'metric': 'a',
                    'value': approx(1.2),
                    'timestamp': 1626375600000,  # ms value for datetime above
                }],
            },
        }


async def test_stream_invalid(app, client, m_victoria):
    async with client.ws_connect('/timeseries/stream') as ws:
        ws: ClientWebSocketResponse
        await ws.send_json({'empty': True})
        resp = await ws.receive_json(timeout=2)
        assert resp['error']


async def test_stream_close(app, client, m_victoria):
    async with client.ws_connect('/timeseries/stream') as ws:
        ws: ClientWebSocketResponse
        await socket_closer.fget(app).before_shutdown(app)
        resp = await ws.receive(timeout=1)
        assert resp.data == WSCloseCode.OK
