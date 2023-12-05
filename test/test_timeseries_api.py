"""
Tests brewblox_history.timeseries_api
"""

from datetime import datetime, timezone
from time import time_ns
from unittest.mock import ANY, AsyncMock, Mock

import pytest
from fastapi import FastAPI
from httpx import AsyncClient
from pytest import approx
from pytest_mock import MockerFixture
from starlette.testclient import TestClient, WebSocketTestSession

from brewblox_history import app_factory, timeseries_api, utils
from brewblox_history.models import (ServiceConfig, TimeSeriesCsvQuery,
                                     TimeSeriesMetric, TimeSeriesRange,
                                     TimeSeriesRangeMetric,
                                     TimeSeriesRangeValue)

TESTED = timeseries_api.__name__


class dt_eq:

    def __init__(self, value: utils.DatetimeSrc_) -> None:
        self.value = utils.parse_datetime(value)

    def __eq__(self, __value: object) -> bool:
        return self.value == utils.parse_datetime(__value)


@pytest.fixture
async def m_victoria(mocker: MockerFixture) -> Mock:
    m = mocker.patch(TESTED + '.victoria.CV').get.return_value
    m.ping = AsyncMock()
    m.fields = AsyncMock()
    m.metrics = AsyncMock()
    m.ranges = AsyncMock()
    m.csv = AsyncMock()
    return m


@pytest.fixture
def app() -> FastAPI:
    app = FastAPI()
    app.include_router(timeseries_api.router)
    app_factory.add_exception_handlers(app)
    return app


async def test_ping(client: AsyncClient, m_victoria: Mock):
    resp = await client.get('/timeseries/ping')
    assert resp.status_code == 200
    assert resp.json() == {'ping': 'pong'}

    m_victoria.ping.side_effect = RuntimeError
    with pytest.raises(RuntimeError):
        await client.get('/timeseries/ping')


async def test_fields(client: AsyncClient, m_victoria: Mock):
    m_victoria.fields.return_value = ['a', 'b', 'c']

    resp = await client.post('/timeseries/fields', json={'duration': '1d'})
    assert resp.json() == ['a', 'b', 'c']

    resp = await client.post('/timeseries/fields', json={'duration': []})
    assert resp.status_code == 422


async def test_ranges(client: AsyncClient, m_victoria: Mock):
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

    resp = await client.post('/timeseries/ranges', json={'fields': ['a', 'b', 'c']})
    assert resp.json() == [
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

    resp = await client.post('/timeseries/ranges', json={})
    assert resp.status_code == 422


async def test_metrics(client: AsyncClient, m_victoria: Mock):
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

    resp = await client.post('/timeseries/metrics', json={'fields': ['a', 'b', 'c']})
    assert resp.json() == [
        {'metric': 'a', 'value': approx(1.2), 'timestamp': dt_eq(now)},
        {'metric': 'b', 'value': approx(2.2), 'timestamp': dt_eq(now)},
        {'metric': 'c', 'value': approx(3.2), 'timestamp': dt_eq(now)},
    ]

    resp = await client.post('/timeseries/metrics', json={})
    assert resp.status_code == 422


async def test_csv(client: AsyncClient, m_victoria: Mock, mocker: MockerFixture):
    mocker.patch(TESTED + '.CSV_CHUNK_SIZE', 10)

    async def csv_mock(args: TimeSeriesCsvQuery):
        yield ','.join(args.fields)
        yield 'line 1'
        yield 'line 2'

    m_victoria.csv = csv_mock

    resp = await client.post('/timeseries/csv',
                             json={'fields': ['a', 'b', 'c'], 'precision': 's'})
    assert resp.text == 'a,b,c\nline 1\nline 2\n'

    resp = await client.post('/timeseries/csv', json={})
    assert resp.status_code == 422


async def test_empty_csv(client: AsyncClient, m_victoria: Mock):
    async def csv_mock(args: TimeSeriesCsvQuery):
        yield ','.join(args.fields)

    m_victoria.csv = csv_mock

    resp = await client.post('/timeseries/csv',
                             json={'fields': ['a', 'b', 'c'], 'precision': 's'})
    assert resp.text == 'a,b,c\n'


async def test_stream(sync_client: TestClient, config: ServiceConfig, m_victoria: Mock):
    config.ranges_interval = 0.001
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

    with sync_client.websocket_connect('/timeseries/stream') as ws:
        ws: WebSocketTestSession

        # Metrics
        ws.send_json({
            'id': 'test-metrics',
            'command': 'metrics',
            'query': {
                'fields': ['a', 'b', 'c'],
            },
        })
        resp = ws.receive_json()
        assert resp == {
            'id': 'test-metrics',
            'data': {
                'metrics': [ANY, ANY, ANY],
            },
        }

        # Ranges
        ws.send_json({
            'id': 'test-ranges-once',
            'command': 'ranges',
            'query': {
                'fields': ['a', 'b', 'c'],
                'end': '2021-07-15T14:29:30.000Z',
            },
        })
        resp = ws.receive_json()
        assert resp == {
            'id': 'test-ranges-once',
            'data': {
                'initial': True,
                'ranges': [ANY, ANY, ANY],
            },
        }

        # Live ranges
        ws.send_json({
            'id': 'test-ranges-live',
            'command': 'ranges',
            'query': {
                'fields': ['a', 'b', 'c'],
                'duration': '30m',
            },
        })
        resp = ws.receive_json()
        assert resp == {
            'id': 'test-ranges-live',
            'data': {
                'initial': True,
                'ranges': [ANY, ANY, ANY],
            },
        }
        resp = ws.receive_json()
        assert resp == {
            'id': 'test-ranges-live',
            'data': {
                'initial': False,
                'ranges': [ANY, ANY, ANY],
            },
        }

        # Stop live ranges
        ws.send_json({
            'id': 'test-ranges-live',
            'command': 'stop',
        })


async def test_stream_error(sync_client: TestClient, m_victoria: Mock):
    dt = datetime(2021, 7, 15, 19, tzinfo=timezone.utc)
    m_victoria.ranges.side_effect = RuntimeError
    m_victoria.metrics.return_value = [
        TimeSeriesMetric(
            metric='a',
            value=1.2,
            timestamp=dt
        ),
    ]

    with sync_client.websocket_connect('/timeseries/stream') as ws:
        ws: WebSocketTestSession

        # Invalid request
        ws.send_json({'empty': True})
        resp = ws.receive_json()
        assert resp['error']

        # Backend raises error
        ws.send_json({
            'id': 'test-ranges-once',
            'command': 'ranges',
            'query': {
                'fields': ['a', 'b', 'c'],
                'end': '2021-07-15T14:29:30.000Z',
            },
        })

        # Other command is OK
        ws.send_json({
            'id': 'test-metrics',
            'command': 'metrics',
            'query': {
                'fields': ['a', 'b', 'c'],
            },
        })

        resp = ws.receive_json()
        assert resp == {
            'id': 'test-metrics',
            'data': {
                'metrics': [{
                    'metric': 'a',
                    'value': approx(1.2),
                    'timestamp': dt_eq(dt),
                }],
            },
        }
