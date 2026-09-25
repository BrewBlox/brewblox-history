"""
Tests brewblox_history.downsample
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs

import httpx
import pytest
from httpx import Request, Response
from pytest_httpx import HTTPXMock
from pytest_mock import MockerFixture

from brewblox_history import downsample, utils, victoria
from brewblox_history.models import ServiceConfig

TESTED = downsample.__name__

# 2021-07-15 19:00 UTC, on the minute
NOW = 1626375600
HOUR = 3600
DAY = 24 * HOUR
RETENTION = 30 * DAY
MARKER_QUERY = f'max(tlast_over_time({{__name__="{victoria.MARKER}"}}'


@pytest.fixture
def url(config: ServiceConfig) -> str:
    return f'{config.victoria_protocol}://{config.victoria_host}:{config.victoria_port}{config.victoria_path}'


@pytest.fixture
def dense_url(config: ServiceConfig) -> str:
    return f'{config.dense_protocol}://{config.dense_host}:{config.dense_port}{config.dense_path}'


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """The time, in Unix seconds: set clock[0] to move it."""
    clock = [NOW]
    monkeypatch.setattr(utils, 'now', lambda: datetime.fromtimestamp(clock[0], timezone.utc))
    return clock


@pytest.fixture
def ds(config: ServiceConfig, clock: list[int]) -> downsample.Downsampler:
    config.dense_enabled = True
    victoria.setup()
    downsample.setup()
    return downsample.CV.get()


def params(request: Request) -> dict[str, str]:
    return {k: v[0] for k, v in parse_qs(request.read().decode()).items()}


def vector(value: float | None) -> Response:
    result = [] if value is None else [{'metric': {}, 'value': [NOW, str(value)]}]
    return Response(200, json={'status': 'success', 'data': {'resultType': 'vector', 'result': result}})


def matrix(result: list[dict]) -> Response:
    return Response(200, json={'status': 'success', 'data': {'resultType': 'matrix', 'result': result}})


def averages_handler(request: Request) -> Response:
    """Dense averages: series 'a' and 'b"' at every point from start to end, valued by timestamp."""
    p = params(request)
    start, end, step = int(p['start']), int(p['end']), int(p['step'].rstrip('s'))
    return matrix(
        [
            {'metric': {'__name__': name}, 'values': [[t, str(t % 1000 + offset)] for t in range(start, end + 1, step)]}
            for offset, name in enumerate(['a', 'b"'])
        ]
    )


class FakeDense:
    """A dense database with a sample every second in each range (first, last),
    answering the per-day index and tfirst queries."""

    def __init__(self, *ranges: tuple[int, int]):
        self.ranges = list(ranges)

    def series(self, request: Request) -> Response:
        p = params(request)
        # The per-day index: whole days, inclusive
        start_day = int(p['start']) - int(p['start']) % DAY
        end_day = int(p['end']) - int(p['end']) % DAY
        found = any(first < end_day + DAY and last >= start_day for first, last in self.ranges)
        data = [{'__name__': 'a'}] if found else []
        return Response(200, json={'status': 'success', 'data': data})

    def query(self, request: Request) -> Response:
        p = params(request)
        window = int(p['query'].split('[')[1].split('s]')[0])
        t = int(p['time'])
        # The oldest sample in (t - window, t]
        candidates = [max(first, t - window + 1) for first, _ in self.ranges]
        found = [c for c, (_, last) in zip(candidates, self.ranges, strict=True) if c <= min(last, t)]
        return vector(min(found) if found else None)


def imported(httpx_mock: HTTPXMock, url: str) -> list[list[dict]]:
    """The import requests to the long-term database, as parsed JSON lines."""
    return [
        [json.loads(line) for line in r.read().decode().splitlines()]
        for r in httpx_mock.get_requests(url=f'{url}/api/v1/import')
    ]


def marker(cursor: int) -> dict:
    return {'metric': {'__name__': victoria.MARKER}, 'values': [cursor], 'timestamps': [cursor * 1000]}


def test_import_lines():
    body = json.dumps(
        {
            'data': {
                'result': [
                    {'metric': {'__name__': 'a "b"'}, 'values': [[1626375540, '1.5'], [1626375600, '2']]},
                    # Not finite: the database would skip the whole line
                    {'metric': {'__name__': 'c'}, 'values': [[1626375540, '+Inf'], [1626375600, '3']]},
                    {'metric': {'__name__': 'd'}, 'values': [[1626375540, '-Inf']]},
                ]
            }
        }
    ).encode()
    assert [json.loads(line) for line in downsample.import_lines(body)] == [
        {'metric': {'__name__': 'a "b"'}, 'values': [1.5, 2.0], 'timestamps': [1626375540000, 1626375600000]},
        {'metric': {'__name__': 'c'}, 'values': [3.0], 'timestamps': [1626375600000]},
    ]
    assert json.loads(downsample.marker_line(NOW)) == marker(NOW)


@pytest.mark.parametrize(
    'ranges, expected, scans',
    [
        # Samples across the whole retention: nothing to report, found at once
        ([(NOW - RETENTION - 100, NOW)], None, 1),
        # Within the margin of the retention's start: nothing to report either
        ([(NOW - RETENTION + 1800, NOW)], None, 1),
        # Created or wiped later: from its first sample (20:23 three days ago: the 21st hour of that day)
        ([(NOW - 3 * DAY + 5000, NOW)], NOW - 3 * DAY + 5000, 21),
        ([(NOW - 100, NOW)], NOW - 100, 19),
        # Samples on the retention's first day, but all before it starts: that day is scanned, then skipped
        ([(NOW - RETENTION - 3600, NOW - RETENTION - 100), (NOW - 2 * DAY, NOW)], NOW - 2 * DAY, 5 + 19),
        # No samples at all: they start now
        ([], NOW, 0),
    ],
)
async def test_find_dense_since(
    ds: downsample.Downsampler,
    dense_url: str,
    httpx_mock: HTTPXMock,
    ranges: list[tuple[int, int]],
    expected: int | None,
    scans: int,
):
    dense = FakeDense(*ranges)
    httpx_mock.add_callback(url=f'{dense_url}/api/v1/series', method='POST', callback=dense.series, is_reusable=True)
    httpx_mock.add_callback(
        url=f'{dense_url}/api/v1/query', method='POST', callback=dense.query, is_reusable=True, is_optional=True
    )
    assert await ds.find_dense_since(NOW) == expected

    # Days come from the index, one lookup each, up to the first with series.
    # Only that day is scanned, an hour at a time, up to the first sample.
    lookups = [params(r) for r in httpx_mock.get_requests(url=f'{dense_url}/api/v1/series')]
    assert len(lookups) <= 31
    assert all(int(p['end']) - int(p['start']) < DAY for p in lookups)
    queries = [params(r) for r in httpx_mock.get_requests(url=f'{dense_url}/api/v1/query')]
    assert len(queries) == scans
    assert all(p['query'] == f'min(tfirst_over_time({{__name__!=""}}[{HOUR + 1}s]))' for p in queries)


async def test_discover_cursor(
    ds: downsample.Downsampler, url: str, httpx_mock: HTTPXMock, caplog: pytest.LogCaptureFixture
):
    # From the marker: the end of the last averages
    httpx_mock.add_callback(url=f'{url}/api/v1/query', method='POST', callback=lambda _: vector(NOW - 120))
    assert await ds.discover_cursor(NOW, None) == NOW - 120
    assert ds.marked == NOW - 120
    p = params(httpx_mock.get_requests()[-1])
    assert p['query'] == f'{MARKER_QUERY}[{RETENTION}s]))'
    assert (p['time'], p['nocache']) == (str(NOW), '1')

    # Where the dense database's samples start, if later: the interval holding its first sample
    httpx_mock.add_callback(url=f'{url}/api/v1/query', method='POST', callback=lambda _: vector(NOW - 7200))
    assert await ds.discover_cursor(NOW, NOW - 3570) == NOW - 3600
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]

    # No marker in the dense retention: from its start, or where its samples start
    httpx_mock.add_callback(url=f'{url}/api/v1/query', method='POST', callback=lambda _: vector(None), is_reusable=True)
    httpx_mock.add_response(url=f'{url}/api/v1/series', method='POST', json={'data': []}, is_reusable=True)
    assert await ds.discover_cursor(NOW, None) == NOW - RETENTION
    assert await ds.discover_cursor(NOW, NOW - 125) == NOW - 180
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
    # The whole index is asked for the marker, without reading samples
    p = params(httpx_mock.get_requests(url=f'{url}/api/v1/series')[-1])
    assert p == {'match[]': f'{{__name__="{victoria.MARKER}"}}', 'start': '1', 'end': str(NOW), 'limit': '1'}


async def test_discover_cursor_old_marker(
    ds: downsample.Downsampler, url: str, httpx_mock: HTTPXMock, caplog: pytest.LogCaptureFixture
):
    # Averages from before the dense retention: the dense database no longer has the time since, with a warning
    httpx_mock.add_callback(url=f'{url}/api/v1/query', method='POST', callback=lambda _: vector(None))
    httpx_mock.add_response(url=f'{url}/api/v1/series', method='POST', json={'data': [{'__name__': victoria.MARKER}]})
    assert await ds.discover_cursor(NOW + 30, None) == NOW - RETENTION + 60
    assert 'the time between is not averaged' in caplog.text


async def test_check_archive(
    ds: downsample.Downsampler, url: str, httpx_mock: HTTPXMock, caplog: pytest.LogCaptureFixture
):
    vic = victoria.CV.get()

    # Nothing marked yet: nothing to check
    await ds.check_archive(NOW)
    assert httpx_mock.get_requests() == []

    # The marker reaches what was marked
    ds.cursor = ds.marked = vic.cursor = NOW - 300
    httpx_mock.add_callback(url=f'{url}/api/v1/query', method='POST', callback=lambda _: vector(NOW - 300))
    await ds.check_archive(NOW)
    assert ds.cursor == NOW - 300
    assert params(httpx_mock.get_requests()[-1])['query'] == f'{MARKER_QUERY}[301s]))'

    # It does not: the long-term database lost imports, and they are averaged again
    ds._publish_cursor(NOW - 240, 10)
    httpx_mock.add_callback(url=f'{url}/api/v1/query', method='POST', callback=lambda _: vector(NOW - 900))
    await ds.check_archive(NOW)
    assert (ds.cursor, ds.marked, vic.cursor) == (None, None, None)
    assert not ds._pending
    assert 'The long-term database lost averages' in caplog.text

    # No marker at all
    ds.cursor = ds.marked = vic.cursor = NOW - 300
    httpx_mock.add_callback(url=f'{url}/api/v1/query', method='POST', callback=lambda _: vector(None))
    await ds.check_archive(NOW)
    assert ds.cursor is None


async def test_downsample(
    ds: downsample.Downsampler,
    url: str,
    dense_url: str,
    httpx_mock: HTTPXMock,
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(downsample, 'SEARCHABLE_DELAY', timedelta(milliseconds=50))
    httpx_mock.add_callback(
        url=f'{dense_url}/api/v1/query_range', method='POST', callback=averages_handler, is_reusable=True
    )
    httpx_mock.add_response(url=f'{url}/api/v1/import', method='POST', status_code=204, is_reusable=True)

    # Intervals end at least downsample_lag (30 s) ago: at NOW + 29, the last one ended at NOW - 60
    ds.cursor = NOW - 180
    await ds.downsample(NOW + 29)
    assert ds.cursor == NOW - 60
    [query] = httpx_mock.get_requests(url=f'{dense_url}/api/v1/query_range')
    p = params(query)
    assert p['query'] == 'avg_over_time({__name__!=""}[60s]) keep_metric_names'
    assert (p['start'], p['end'], p['step']) == (str(NOW - 120), str(NOW - 60), '60s')
    assert (p['nocache'], p['latency_offset']) == ('1', '30.0')
    timestamps = [(NOW - 120) * 1000, (NOW - 60) * 1000]
    assert imported(httpx_mock, url) == [
        [
            {'metric': {'__name__': 'a'}, 'values': [480.0, 540.0], 'timestamps': timestamps},
            {'metric': {'__name__': 'b"'}, 'values': [481.0, 541.0], 'timestamps': timestamps},
            marker(NOW - 60),
        ]
    ]

    # Nothing new until the next interval is downsample_lag old
    await ds.downsample(NOW + 29)
    assert len(imported(httpx_mock, url)) == 1

    # Reads get the cursor once the averages are searchable, and the marker check expects it
    vic = victoria.CV.get()
    assert (vic.cursor, ds.marked) == (None, None)
    await asyncio.sleep(0.1)
    assert (vic.cursor, ds.marked) == (NOW - 60, NOW - 60)


async def test_downsample_catch_up(
    ds: downsample.Downsampler,
    config: ServiceConfig,
    url: str,
    dense_url: str,
    httpx_mock: HTTPXMock,
):
    # Behind by more than a chunk: chunks of downsample_chunk, rounded down to the interval
    config.downsample_chunk = timedelta(minutes=10, seconds=30)
    httpx_mock.add_callback(
        url=f'{dense_url}/api/v1/query_range', method='POST', callback=averages_handler, is_reusable=True
    )
    httpx_mock.add_response(url=f'{url}/api/v1/import', method='POST', status_code=204, is_reusable=True)

    ds.cursor = NOW - 1800
    await ds.downsample(NOW + 30)
    windows = [
        (int(p['start']), int(p['end']))
        for p in map(params, httpx_mock.get_requests(url=f'{dense_url}/api/v1/query_range'))
    ]
    assert windows == [(NOW - 1740, NOW - 1200), (NOW - 1140, NOW - 600), (NOW - 540, NOW)]
    assert [lines[-1] for lines in imported(httpx_mock, url)] == [marker(NOW - 1200), marker(NOW - 600), marker(NOW)]
    assert ds.cursor == NOW


async def test_downsample_empty(ds: downsample.Downsampler, url: str, dense_url: str, httpx_mock: HTTPXMock):
    # No samples in the window: only the marker moves on
    httpx_mock.add_callback(url=f'{dense_url}/api/v1/query_range', method='POST', callback=lambda _: matrix([]))
    httpx_mock.add_response(url=f'{url}/api/v1/import', method='POST', status_code=204)
    ds.cursor = NOW - 120
    await ds.downsample(NOW + 30)
    assert ds.cursor == NOW
    assert imported(httpx_mock, url) == [[marker(NOW)]]


async def test_downsample_failures(
    ds: downsample.Downsampler,
    url: str,
    dense_url: str,
    httpx_mock: HTTPXMock,
    monkeypatch: pytest.MonkeyPatch,
):
    # The cursor only moves after a successful import, and reads never see it move
    monkeypatch.setattr(downsample, 'SEARCHABLE_DELAY', timedelta(milliseconds=10))
    vic = victoria.CV.get()
    ds.cursor = NOW - 120
    httpx_mock.add_exception(
        url=f'{dense_url}/api/v1/query_range', method='POST', exception=httpx.ConnectError('refused')
    )
    with pytest.raises(ConnectionError):
        await ds.downsample(NOW + 30)
    assert ds.cursor == NOW - 120

    httpx_mock.add_callback(url=f'{dense_url}/api/v1/query_range', method='POST', callback=averages_handler)
    httpx_mock.add_response(url=f'{url}/api/v1/import', method='POST', status_code=503)
    with pytest.raises(httpx.HTTPStatusError):
        await ds.downsample(NOW + 30)
    assert ds.cursor == NOW - 120
    assert not ds._pending
    await asyncio.sleep(0.05)
    assert (vic.cursor, ds.marked) == (None, None)


async def test_downsample_lag_rounded_up(
    ds: downsample.Downsampler, config: ServiceConfig, url: str, dense_url: str, httpx_mock: HTTPXMock
):
    # An interval is averaged at least downsample_lag after it ended, also for a lag in fractions of a second
    config.downsample_lag = timedelta(seconds=30.5)
    httpx_mock.add_callback(url=f'{dense_url}/api/v1/query_range', method='POST', callback=averages_handler)
    httpx_mock.add_response(url=f'{url}/api/v1/import', method='POST', status_code=204)
    ds.cursor = NOW - 120
    await ds.downsample(NOW + 30)
    assert ds.cursor == NOW - 60


async def test_downsample_chunk_interval(
    ds: downsample.Downsampler, config: ServiceConfig, url: str, dense_url: str, httpx_mock: HTTPXMock
):
    # A chunk is at least one interval
    config.sparse_interval = timedelta(minutes=10)
    config.downsample_chunk = timedelta(minutes=1)
    httpx_mock.add_callback(
        url=f'{dense_url}/api/v1/query_range', method='POST', callback=averages_handler, is_reusable=True
    )
    httpx_mock.add_response(url=f'{url}/api/v1/import', method='POST', status_code=204, is_reusable=True)
    ds.cursor = NOW - 1800
    await ds.downsample(NOW + 30)
    windows = [
        (int(p['start']), int(p['end']))
        for p in map(params, httpx_mock.get_requests(url=f'{dense_url}/api/v1/query_range'))
    ]
    assert windows == [(NOW - 1200, NOW - 1200), (NOW - 600, NOW - 600), (NOW, NOW)]


async def test_check_lag(ds: downsample.Downsampler, caplog: pytest.LogCaptureFixture):
    caplog.set_level(logging.INFO, logger=TESTED)

    def messages() -> list[str]:
        return [r.getMessage().split(':')[0] for r in caplog.records if r.name == TESTED]

    # Not started
    assert ds.age(NOW) is None
    ds.check_lag(NOW)

    # Until the cursor is known, the lag counts from the start
    ds.started = NOW - 700
    assert ds.age(NOW) == 700
    ds.check_lag(NOW)
    assert messages() == ['Downsampling is behind']

    ds.cursor = NOW - 600
    ds.check_lag(NOW)
    assert messages() == ['Downsampling is behind', 'Downsampling caught up']

    # Warned once while more than downsample_max_lag (10 min) behind
    ds.check_lag(NOW + 1)
    ds.check_lag(NOW + 100)
    assert messages() == ['Downsampling is behind', 'Downsampling caught up', 'Downsampling is behind']


async def test_max_lag(ds: downsample.Downsampler, config: ServiceConfig):
    # downsample_max_lag, or more when a long interval makes the averages end further back when keeping up
    assert ds.max_lag() == 600
    config.sparse_interval = timedelta(minutes=10)
    assert ds.max_lag() == 30 + 1200 + 15


async def test_tick(
    ds: downsample.Downsampler,
    url: str,
    dense_url: str,
    clock: list[int],
    httpx_mock: HTTPXMock,
):
    dense = FakeDense((NOW - 300, NOW + 2 * HOUR))
    httpx_mock.add_callback(url=f'{dense_url}/api/v1/series', method='POST', callback=dense.series, is_reusable=True)
    httpx_mock.add_callback(url=f'{dense_url}/api/v1/query', method='POST', callback=dense.query, is_reusable=True)
    httpx_mock.add_callback(
        url=f'{url}/api/v1/query', method='POST', callback=lambda _: vector(NOW - 7200), is_reusable=True
    )
    httpx_mock.add_callback(
        url=f'{dense_url}/api/v1/query_range', method='POST', callback=averages_handler, is_reusable=True
    )
    httpx_mock.add_response(url=f'{url}/api/v1/import', method='POST', status_code=204, is_reusable=True)

    # The first tick finds where the dense database starts and where the averages end, then averages
    await ds.tick()
    vic = victoria.CV.get()
    assert vic.dense_since == NOW - 300
    assert ds.cursor == NOW - 60
    # The cursor it found is already searchable
    assert vic.cursor == NOW - 300

    # Where the dense database starts is looked for again after an hour
    dense.ranges = [(NOW - 200, NOW + 2 * HOUR)]
    clock[0] = NOW + 30
    await ds.tick()
    assert vic.dense_since == NOW - 300
    clock[0] = NOW + HOUR
    await ds.tick()
    assert vic.dense_since == NOW - 200


async def test_tick_rewind(
    ds: downsample.Downsampler,
    url: str,
    dense_url: str,
    httpx_mock: HTTPXMock,
    caplog: pytest.LogCaptureFixture,
):
    # The long-term database lost the imports after NOW - 600: the same tick finds the marker and averages again
    ds._dense_since_at = NOW
    ds.cursor = ds.marked = NOW - 60
    vic = victoria.CV.get()
    vic.cursor = NOW - 60
    httpx_mock.add_callback(
        url=f'{url}/api/v1/query', method='POST', callback=lambda _: vector(NOW - 600), is_reusable=True
    )
    httpx_mock.add_callback(url=f'{dense_url}/api/v1/query_range', method='POST', callback=averages_handler)
    httpx_mock.add_response(url=f'{url}/api/v1/import', method='POST', status_code=204)

    await ds.tick()
    assert 'The long-term database lost averages' in caplog.text
    [query] = httpx_mock.get_requests(url=f'{dense_url}/api/v1/query_range')
    assert (params(query)['start'], params(query)['end']) == (str(NOW - 540), str(NOW - 60))
    assert ds.cursor == NOW - 60
    # Reads only count on the marker until the new averages are searchable
    assert vic.cursor == NOW - 600


async def test_run(
    ds: downsample.Downsampler,
    config: ServiceConfig,
    dense_url: str,
    clock: list[int],
    httpx_mock: HTTPXMock,
    caplog: pytest.LogCaptureFixture,
):
    # Failing ticks are logged, the task keeps going, and it warns when it falls behind
    config.downsample_interval = timedelta(milliseconds=1)
    ds._dense_since_at = NOW
    ds.cursor = NOW - 60
    httpx_mock.add_exception(
        url=f'{dense_url}/api/v1/query_range',
        method='POST',
        exception=httpx.ConnectError('refused'),
        is_reusable=True,
    )
    clock[0] = NOW + HOUR - 1
    task = asyncio.create_task(ds.run())
    await asyncio.sleep(0.05)
    assert not task.done()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert 'Downsampling failed: ConnectionError' in caplog.text
    assert 'Downsampling is behind' in caplog.text
    assert len(httpx_mock.get_requests()) > 1


async def test_lifespan(ds: downsample.Downsampler, config: ServiceConfig, mocker: MockerFixture):
    runs = []

    async def run():
        runs.append(True)
        await asyncio.Event().wait()  # never ends

    mocker.patch.object(ds, 'run', side_effect=run)
    m_stop = mocker.patch.object(ds, 'stop', autospec=True)

    # Without the dense database, no task
    config.dense_enabled = False
    async with downsample.lifespan():
        pass
    assert runs == []

    # The task runs until the service stops, and is then cancelled
    config.dense_enabled = True
    async with asyncio.timeout(1):
        async with downsample.lifespan():
            await asyncio.sleep(0)
    assert runs == [True]
    m_stop.assert_called_once()


async def test_stop(ds: downsample.Downsampler):
    # Cursors waiting to become searchable are dropped
    ds._publish_cursor(NOW, 0.05)
    ds.stop()
    await asyncio.sleep(0.1)
    assert victoria.CV.get().cursor is None
