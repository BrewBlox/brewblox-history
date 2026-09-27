"""
Tests brewblox_history.timeseries_api
"""

import asyncio
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from unittest.mock import ANY, AsyncMock, Mock

import pytest
from asgi_lifespan import LifespanManager
from fastapi import FastAPI
from httpx import AsyncClient
from httpx_ws import aconnect_ws
from httpx_ws.transport import ASGIWebSocketTransport
from pytest_mock import MockerFixture

from brewblox_history import app_factory, downsample, timeseries_api, utils
from brewblox_history.models import (
    ServiceConfig,
    TimeSeriesCsvQuery,
    TimeSeriesMetric,
    TimeSeriesRange,
    TimeSeriesRangeMetric,
    TimeSeriesRangeValue,
)
from brewblox_history.planner import FollowUp

TESTED = timeseries_api.__name__


class DtEq:
    """Equal to any value that parses to the same datetime."""

    def __init__(self, value: utils.DatetimeSrc_) -> None:
        self.value = utils.parse_datetime(value)

    def __eq__(self, other: object, /) -> bool:
        if not isinstance(other, str | int | float | datetime):
            return NotImplemented
        return self.value == utils.parse_datetime(other)

    def __hash__(self) -> int:
        return hash(self.value)


@pytest.fixture
async def m_victoria(mocker: MockerFixture) -> Mock:
    m = mocker.patch(TESTED + '.victoria.CV').get.return_value
    m.ping = AsyncMock()
    m.fields = AsyncMock()
    m.metrics = AsyncMock()
    m.ranges = AsyncMock()
    m.initial_ranges = AsyncMock()
    m.follow_up_ranges = AsyncMock()
    m.csv = AsyncMock()
    return m


@pytest.fixture
def app() -> FastAPI:
    downsample.setup()
    app = FastAPI()
    app.include_router(timeseries_api.router)
    app_factory.add_exception_handlers(app)
    return app


async def test_ping(client: AsyncClient, m_victoria: Mock, mocker: MockerFixture):
    resp = await client.get('/timeseries/ping')
    assert resp.status_code == 200
    assert resp.json() == {'ping': 'pong', 'downsample_age': None}

    # How old the long-term database's averages are
    now = datetime(2021, 7, 15, 19, tzinfo=UTC)
    mocker.patch(TESTED + '.utils.now').return_value = now
    downsample.CV.get().cursor = int(now.timestamp()) - 90
    resp = await client.get('/timeseries/ping')
    assert resp.json() == {'ping': 'pong', 'downsample_age': 90}

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
        TimeSeriesRange(metric=TimeSeriesRangeMetric(__name__='a'), values=[TimeSeriesRangeValue(1234, '54321')]),
        TimeSeriesRange(metric=TimeSeriesRangeMetric(__name__='b'), values=[TimeSeriesRangeValue(2345, '54321')]),
        TimeSeriesRange(metric=TimeSeriesRangeMetric(__name__='c'), values=[TimeSeriesRangeValue(3456, '54321')]),
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
    now = datetime.now(UTC)
    m_victoria.metrics.return_value = [
        TimeSeriesMetric(metric='a', value=1.2, timestamp=now),
        TimeSeriesMetric(metric='b', value=2.2, timestamp=now),
        TimeSeriesMetric(metric='c', value=3.2, timestamp=now),
    ]

    resp = await client.post('/timeseries/metrics', json={'fields': ['a', 'b', 'c']})
    assert resp.json() == [
        {'metric': 'a', 'value': pytest.approx(1.2), 'timestamp': DtEq(now)},
        {'metric': 'b', 'value': pytest.approx(2.2), 'timestamp': DtEq(now)},
        {'metric': 'c', 'value': pytest.approx(3.2), 'timestamp': DtEq(now)},
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

    resp = await client.post('/timeseries/csv', json={'fields': ['a', 'b', 'c'], 'precision': 's'})
    assert resp.text == 'a,b,c\nline 1\nline 2\n'

    resp = await client.post('/timeseries/csv', json={})
    assert resp.status_code == 422


async def test_empty_csv(client: AsyncClient, m_victoria: Mock):
    async def csv_mock(args: TimeSeriesCsvQuery):
        yield ','.join(args.fields)

    m_victoria.csv = csv_mock

    resp = await client.post('/timeseries/csv', json={'fields': ['a', 'b', 'c'], 'precision': 's'})
    assert resp.text == 'a,b,c\n'


async def receive(ws, stream_id: str, received: dict[str, list[dict]]) -> dict:
    """
    Receive the next message of stream `stream_id`.
    Streams share the socket, so messages of other streams may come first:
    all messages are kept in `received`, by stream id.
    """
    try:
        async with asyncio.timeout(1):
            while True:
                msg = await ws.receive_json()
                received[msg['id']].append(msg)
                if msg['id'] == stream_id:
                    return msg
    except TimeoutError:
        pytest.fail(f'No message for {stream_id}, received: { {k: len(v) for k, v in received.items()} }')


def one_range(name: str, *timestamps: int) -> TimeSeriesRange:
    return TimeSeriesRange(
        metric=TimeSeriesRangeMetric(__name__=name),
        values=[TimeSeriesRangeValue(t, '1') for t in timestamps],
    )


def range_json(name: str, *timestamps: int) -> dict:
    return {'metric': {'__name__': name}, 'values': [[t, '1'] for t in timestamps]}


async def test_stream(app: FastAPI, manager: LifespanManager, config: ServiceConfig, m_victoria: Mock):
    config.ranges_interval = timedelta(milliseconds=1)
    config.metrics_interval = timedelta(milliseconds=1)
    m_victoria.metrics.return_value = [
        TimeSeriesMetric(metric='a', value=1.2, timestamp=datetime.fromtimestamp(1, UTC)),
        TimeSeriesMetric(metric='b', value=2.2, timestamp=datetime.fromtimestamp(1, UTC)),
        TimeSeriesMetric(metric='c', value=3.2, timestamp=datetime.fromtimestamp(1, UTC)),
    ]
    m_victoria.initial_ranges.return_value = (
        [one_range('a', 90, 100), one_range('b', 100)],
        FollowUp(100, 10, 100),
    )

    # The fields and where each follow-up was asked to continue
    asked: list[tuple[list[str], FollowUp]] = []

    async def follow_up(fields: list[str], follow: FollowUp) -> tuple[list, FollowUp]:
        asked.append((fields, follow))
        # Every other tick, nothing new
        if fields != ['a', 'b'] or len(asked) % 2:
            return [], follow
        return [one_range('b', follow.last + 10)], follow._replace(last=follow.last + 10, until=follow.last + 10)

    m_victoria.follow_up_ranges.side_effect = follow_up

    received = defaultdict(list)

    async with (
        AsyncClient(base_url='http://test', transport=ASGIWebSocketTransport(app)) as client,
        aconnect_ws('/timeseries/stream', client) as ws,
    ):
        # Metrics are pushed every interval
        await ws.send_json(
            {
                'id': 'test-metrics',
                'command': 'metrics',
                'query': {
                    'fields': ['a', 'b', 'c'],
                },
            }
        )
        for _ in range(2):
            resp = await receive(ws, 'test-metrics', received)
            assert resp == {
                'id': 'test-metrics',
                'data': {
                    'metrics': [ANY, ANY, ANY],
                },
            }

        # Ranges with an end: sent once
        await ws.send_json(
            {
                'id': 'test-ranges-once',
                'command': 'ranges',
                'query': {
                    'fields': ['c'],
                    'end': '2021-07-15T14:29:30.000Z',
                },
            }
        )
        resp = await receive(ws, 'test-ranges-once', received)
        assert resp == {
            'id': 'test-ranges-once',
            'data': {
                'initial': True,
                'ranges': [range_json('a', 90, 100), range_json('b', 100)],
            },
        }

        # Live ranges: then the new points, each once, in messages only when there are any
        await ws.send_json(
            {
                'id': 'test-ranges-live',
                'command': 'ranges',
                'query': {
                    'fields': ['a', 'b'],
                    'duration': '30m',
                },
            }
        )
        resp = await receive(ws, 'test-ranges-live', received)
        assert resp['data']['initial'] is True
        for timestamp in [110, 120]:
            resp = await receive(ws, 'test-ranges-live', received)
            assert resp == {
                'id': 'test-ranges-live',
                'data': {
                    'initial': False,
                    'ranges': [range_json('b', timestamp)],
                },
            }

        # Stop live ranges
        await ws.send_json(
            {
                'id': 'test-ranges-live',
                'command': 'stop',
            }
        )

    # Each follow-up continues where the previous one ended
    assert [follow for _, follow in asked[:4]] == [(100, 10, 100), (100, 10, 100), (110, 10, 110), (110, 10, 110)]
    # The query with an end had no follow-ups, while the live one had several
    assert len(received['test-ranges-once']) == 1
    assert all(fields == ['a', 'b'] for fields, _ in asked)


async def test_stream_retry(app: FastAPI, manager: LifespanManager, config: ServiceConfig, m_victoria: Mock):
    # A failed query is asked again at the next interval: the initial one as initial, a follow-up from the same point.
    # When the clock went back, the initial ranges again.
    config.ranges_interval = timedelta(milliseconds=1)
    m_victoria.initial_ranges.side_effect = [
        RuntimeError('down'),
        ([], FollowUp(100, 10, 100)),
        ([one_range('a', 50)], FollowUp(50, 10, 50)),
    ]
    asked: list[FollowUp] = []

    async def follow_up(fields: list[str], follow: FollowUp) -> tuple[list, FollowUp | None]:
        asked.append(follow)
        if len(asked) == 1:
            raise RuntimeError('down')
        if len(asked) == 2:
            return [one_range('a', follow.last + 10)], follow._replace(last=follow.last + 10, until=follow.last + 10)
        if len(asked) == 3:
            return [], None
        return [], follow

    m_victoria.follow_up_ranges.side_effect = follow_up
    received = defaultdict(list)

    async with (
        AsyncClient(base_url='http://test', transport=ASGIWebSocketTransport(app)) as client,
        aconnect_ws('/timeseries/stream', client) as ws,
    ):
        await ws.send_json({'id': 'live', 'command': 'ranges', 'query': {'fields': ['a']}})
        resp = await receive(ws, 'live', received)
        assert resp['data'] == {'initial': True, 'ranges': []}
        resp = await receive(ws, 'live', received)
        assert resp['data'] == {'initial': False, 'ranges': [range_json('a', 110)]}
        resp = await receive(ws, 'live', received)
        assert resp['data'] == {'initial': True, 'ranges': [range_json('a', 50)]}
        await ws.send_json({'id': 'live', 'command': 'stop'})

    assert m_victoria.initial_ranges.await_count == 3
    # Starting over, with every field: the UI drops what it holds only for an initial message with ranges
    assert [c.kwargs for c in m_victoria.initial_ranges.await_args_list] == [{}, {}, {'every_field': True}]
    assert asked[:3] == [(100, 10, 100), (100, 10, 100), (110, 10, 110)]


async def test_stream_send_fails(
    app: FastAPI,
    manager: LifespanManager,
    config: ServiceConfig,
    m_victoria: Mock,
    mocker: MockerFixture,
):
    # The stream moves on only after the send: the initial ranges are sent again as initial,
    # and a follow-up is asked again from the same point
    config.ranges_interval = timedelta(milliseconds=1)
    m_victoria.initial_ranges.return_value = ([one_range('a', 100)], FollowUp(100, 10, 100))
    asked: list[FollowUp] = []

    async def follow_up(fields: list[str], follow: FollowUp) -> tuple[list, FollowUp]:
        asked.append(follow)
        return [one_range('a', follow.last + 10)], follow._replace(last=follow.last + 10, until=follow.last + 10)

    m_victoria.follow_up_ranges.side_effect = follow_up

    send = timeseries_api._send_ranges
    sent: list[bool] = []

    async def failing_send(ws, stream_id: str, ranges: list, *, initial: bool) -> None:
        # The first initial and the first follow-up fail
        sent.append(initial)
        if sent.count(initial) == 1:
            raise RuntimeError('closing')
        await send(ws, stream_id, ranges, initial=initial)

    mocker.patch(TESTED + '._send_ranges', failing_send)
    received = defaultdict(list)

    async with (
        AsyncClient(base_url='http://test', transport=ASGIWebSocketTransport(app)) as client,
        aconnect_ws('/timeseries/stream', client) as ws,
    ):
        await ws.send_json({'id': 'live', 'command': 'ranges', 'query': {'fields': ['a']}})
        resp = await receive(ws, 'live', received)
        assert resp['data'] == {'initial': True, 'ranges': [range_json('a', 100)]}
        resp = await receive(ws, 'live', received)
        assert resp['data'] == {'initial': False, 'ranges': [range_json('a', 110)]}
        await ws.send_json({'id': 'live', 'command': 'stop'})

    assert m_victoria.initial_ranges.await_count == 2
    assert asked[:2] == [(100, 10, 100), (100, 10, 100)]


async def test_stream_error(app: FastAPI, manager: LifespanManager, config: ServiceConfig, m_victoria: Mock):
    config.ranges_interval = timedelta(milliseconds=1)
    config.metrics_interval = timedelta(milliseconds=1)
    dt = datetime(2021, 7, 15, 19, tzinfo=UTC)
    m_victoria.initial_ranges.side_effect = RuntimeError
    m_victoria.metrics.return_value = [
        TimeSeriesMetric(metric='a', value=1.2, timestamp=dt),
    ]

    async with (
        AsyncClient(base_url='http://test', transport=ASGIWebSocketTransport(app)) as client,
        aconnect_ws('/timeseries/stream', client) as ws,
    ):
        # Invalid request
        await ws.send_json({'empty': True})
        resp = await ws.receive_json()
        assert resp['error']

        # Backend raises error
        await ws.send_json(
            {
                'id': 'test-ranges-once',
                'command': 'ranges',
                'query': {
                    'fields': ['a', 'b', 'c'],
                    'end': '2021-07-15T14:29:30.000Z',
                },
            }
        )

        # Other command is OK
        await ws.send_json(
            {
                'id': 'test-metrics',
                'command': 'metrics',
                'query': {
                    'fields': ['a', 'b', 'c'],
                },
            }
        )

        resp = await ws.receive_json()
        assert resp == {
            'id': 'test-metrics',
            'data': {
                'metrics': [
                    {
                        'metric': 'a',
                        'value': pytest.approx(1.2),
                        'timestamp': DtEq(dt),
                    }
                ],
            },
        }

        # The query with an end is not asked again, however many intervals pass
        for _ in range(10):
            await ws.receive_json()
        assert m_victoria.initial_ranges.await_count == 1
