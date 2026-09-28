"""
Tests brewblox_history.victoria
"""

import logging
import re
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qs

import ciso8601
import httpx
import pytest
from httpx import Request, Response
from pytest_httpx import HTTPXMock
from pytest_mock import MockerFixture

from brewblox_history import planner, utils, victoria
from brewblox_history.models import (
    HistoryEvent,
    ServiceConfig,
    TimeSeriesCsvQuery,
    TimeSeriesFieldsQuery,
    TimeSeriesMetric,
    TimeSeriesMetricsQuery,
    TimeSeriesRange,
    TimeSeriesRangesQuery,
)

TESTED = victoria.__name__
HOUR = 3600
DAY = 24 * HOUR


@pytest.fixture
def url(config: ServiceConfig) -> str:
    return ''.join(
        [
            config.victoria_protocol,
            '://',
            config.victoria_host,
            ':',
            str(config.victoria_port),
            config.victoria_path,
        ]
    )


@pytest.fixture
def now(mocker: MockerFixture) -> datetime:
    dt = datetime(2021, 7, 15, 19, tzinfo=UTC)
    mocker.patch(TESTED + '.utils.now').side_effect = lambda: dt
    return dt


@pytest.fixture
def dense_url(config: ServiceConfig) -> str:
    return f'{config.dense_protocol}://{config.dense_host}:{config.dense_port}{config.dense_path}'


@pytest.fixture
def vic() -> victoria.VictoriaClient:
    victoria.setup()
    return victoria.CV.get()


@pytest.fixture
def unrefined(monkeypatch: pytest.MonkeyPatch):
    """The first answer only. The mocks of these tests answer a point or two per query:
    mostly empty, which the refinement would ask again for (test_ranges_refined)."""
    monkeypatch.setattr(planner, 'plan_refinement', lambda *_, **__: [])


@pytest.fixture
def written(dense_url: str, httpx_mock: HTTPXMock) -> list[str]:
    written = []

    async def handler(request: Request) -> Response:
        written.append(request.read().decode())
        return Response(200)

    httpx_mock.add_callback(url=f'{dense_url}/write?precision=ms', method='POST', callback=handler, is_reusable=True)
    return written


async def test_ping(vic: victoria.VictoriaClient, url: str, dense_url: str, httpx_mock: HTTPXMock):
    # Both databases must be healthy
    httpx_mock.add_response(url=f'{url}/health', method='GET', text='OK', is_reusable=True)
    httpx_mock.add_response(url=f'{dense_url}/health', method='GET', text='OK')
    await vic.ping()

    # Errors name the database
    httpx_mock.add_response(url=f'{dense_url}/health', method='GET', text='NOK')
    with pytest.raises(ConnectionError, match=f'{dense_url}/: ping returned warning: "NOK"'):
        await vic.ping()

    httpx_mock.add_exception(url=f'{dense_url}/health', method='GET', exception=httpx.ConnectError('refused'))
    with pytest.raises(ConnectionError, match=f'{dense_url}/: ConnectError'):
        await vic.ping()


async def test_ping_both_down(vic: victoria.VictoriaClient, url: str, dense_url: str, httpx_mock: HTTPXMock):
    # Every failing database is reported
    httpx_mock.add_response(url=f'{url}/health', method='GET', text='NOK')
    httpx_mock.add_exception(url=f'{dense_url}/health', method='GET', exception=httpx.ConnectError('refused'))
    with pytest.raises(ConnectionError) as info:
        await vic.ping()
    assert f'{url}/: ping returned warning: "NOK"' in str(info.value)
    assert f'{dense_url}/: ConnectError(refused)' in str(info.value)


async def test_fields(vic: victoria.VictoriaClient, url: str, dense_url: str, httpx_mock: HTTPXMock):
    httpx_mock.add_response(url=f'{dense_url}/api/v1/series', method='POST', json={'status': 'success', 'data': []})
    httpx_mock.add_response(
        url=f'{url}/api/v1/series',
        method='POST',
        json={
            'status': 'success',
            'data': [
                {'__name__': 'spock/setpoint-sensor-pair-2/setting[degC]'},
                {'__name__': 'spock/actuator-1/value'},
                {'__name__': 'sparkey/HERMS MT PID/integralReset'},
                {'__name__': 'sparkey/HERMS HLT PID/inputValue[degC]'},
            ],
        },
    )

    args = TimeSeriesFieldsQuery(duration=timedelta(days=1))
    assert await vic.fields(args) == [
        'sparkey/HERMS HLT PID/inputValue[degC]',
        'sparkey/HERMS MT PID/integralReset',
        'spock/actuator-1/value',
        'spock/setpoint-sensor-pair-2/setting[degC]',
    ]


async def test_fields_union(vic: victoria.VictoriaClient, url: str, dense_url: str, httpx_mock: HTTPXMock):
    # Series from either database: new ones may not be in the long-term database yet
    for db_url, names in [(url, ['b', 'a']), (dense_url, ['c', 'b'])]:
        httpx_mock.add_response(
            url=f'{db_url}/api/v1/series',
            method='POST',
            json={'status': 'success', 'data': [{'__name__': n} for n in names]},
        )

    assert await vic.fields(TimeSeriesFieldsQuery(duration=timedelta(days=1))) == ['a', 'b', 'c']


async def test_fields_marker(vic: victoria.VictoriaClient, url: str, dense_url: str, httpx_mock: HTTPXMock):
    # The downsampler's and the migration's markers are not fields
    names = ['a', victoria.MARKER, victoria.MIGRATION_MARKER]
    httpx_mock.add_response(url=f'{dense_url}/api/v1/series', method='POST', json={'status': 'success', 'data': []})
    httpx_mock.add_response(
        url=f'{url}/api/v1/series',
        method='POST',
        json={'status': 'success', 'data': [{'__name__': n} for n in names]},
    )
    assert await vic.fields(TimeSeriesFieldsQuery(duration=timedelta(days=1))) == ['a']


@pytest.mark.parametrize(
    ('failure', 'logged'),
    [
        ({'exception': httpx.ConnectError('refused')}, 'ConnectionError({dense_url}/: ConnectError(refused))'),
        ({'status_code': 503, 'text': 'too many requests'}, "HTTPStatusError(Server error '503 Service Unavailable'"),
    ],
)
async def test_fields_dense_down(
    vic: victoria.VictoriaClient,
    url: str,
    dense_url: str,
    httpx_mock: HTTPXMock,
    caplog: pytest.LogCaptureFixture,
    failure: dict,
    logged: str,
):
    if 'exception' in failure:
        httpx_mock.add_exception(url=f'{dense_url}/api/v1/series', method='POST', is_reusable=True, **failure)
    else:
        httpx_mock.add_response(url=f'{dense_url}/api/v1/series', method='POST', is_reusable=True, **failure)

    # While the dense database fails, the long-term database's fields are used
    httpx_mock.add_response(
        url=f'{url}/api/v1/series',
        method='POST',
        json={'status': 'success', 'data': [{'__name__': 'a'}]},
    )
    assert await vic.fields(TimeSeriesFieldsQuery(duration=timedelta(days=1))) == ['a']
    assert 'Fields from the long-term database only: ' + logged.format(dense_url=dense_url) in caplog.text

    # Without the long-term database, the request fails
    httpx_mock.add_exception(url=f'{url}/api/v1/series', method='POST', exception=httpx.ConnectError('refused'))
    with pytest.raises(ConnectionError, match=f'{url}/: ConnectError'):
        await vic.fields(TimeSeriesFieldsQuery(duration=timedelta(days=1)))


async def test_metrics(vic: victoria.VictoriaClient, now: datetime, written: list[str]):
    args = TimeSeriesMetricsQuery(fields=['service/f1', 'service/f2'])

    # No values cached yet
    assert await vic.metrics(args) == []

    # Don't return invalid values
    await vic.write(HistoryEvent.model_validate({'key': 'service', 'data': {'f1': 1, 'f2': 'invalid'}}))
    result = await vic.metrics(args)
    assert result == [TimeSeriesMetric(metric='service/f1', value=1, timestamp=now)]

    # Only update new values
    await vic.write(HistoryEvent(key='service', data={'f2': 2}))
    result = await vic.metrics(args)
    assert result == [
        TimeSeriesMetric(metric='service/f1', value=1, timestamp=now),
        TimeSeriesMetric(metric='service/f2', value=2, timestamp=now),
    ]

    # Values carry the event's timestamp if it has one
    sampled = now - timedelta(seconds=5)
    await vic.write(HistoryEvent(key='service', data={'f2': 3}, timestamp=utils.to_millis(sampled)))
    result = await vic.metrics(args)
    assert result == [
        TimeSeriesMetric(metric='service/f1', value=1, timestamp=now),
        TimeSeriesMetric(metric='service/f2', value=3, timestamp=sampled),
    ]

    # Results follow the requested fields, once each
    result = await vic.metrics(TimeSeriesMetricsQuery(fields=['service/f2', 'service/f1', 'service/f2']))
    assert result == [
        TimeSeriesMetric(metric='service/f2', value=3, timestamp=sampled),
        TimeSeriesMetric(metric='service/f1', value=1, timestamp=now),
    ]

    # Values older than the query duration are left out
    args.duration = timedelta(seconds=1)
    result = await vic.metrics(args)
    assert result == [
        TimeSeriesMetric(metric='service/f1', value=1, timestamp=now),
    ]


async def test_ranges(vic: victoria.VictoriaClient, dense_url: str, httpx_mock: HTTPXMock):
    def result(name: str) -> dict:
        return {
            'metric': {'__name__': name},
            'values': [
                [1626367339.856, '1'],
                [1626367349.856, '2'],
                [1626367359.856, '3'],
            ],
        }

    httpx_mock.add_response(
        url=f'{dense_url}/api/v1/query_range',
        method='POST',
        json={
            'status': 'success',
            'data': {
                'resultType': 'matrix',
                'result': [result('f2'), result('f1')],
            },
        },
    )

    # Results follow the requested fields, once each; fields without data are left out
    args = TimeSeriesRangesQuery(fields=['f1', 'f2', 'f3', 'f1'], duration=timedelta(hours=1))
    assert await vic.ranges(args) == [TimeSeriesRange(**result('f1')), TimeSeriesRange(**result('f2'))]

    # One request to the dense database for all fields, which ends query_latency before now and passes it as latency
    # offset
    [request] = httpx_mock.get_requests()
    params = parse_qs(request.read().decode())
    assert params['query'] == ['avg_over_time({__name__="f1" or __name__="f2" or __name__="f3"}[3s]) keep_metric_names']
    assert params['step'] == ['3s']
    assert params['latency_offset'] == ['5.0']
    assert 'nocache' not in params
    # The start is on the grid of the step, at most a step before now - 1h
    assert int(params['start'][0]) % 3 == 0
    assert int(params['end'][0]) - int(params['start'][0]) in range(3595, 3595 + 3)


def series(name: str, values: list) -> TimeSeriesRange:
    """A range from the database's JSON values: [timestamp, value] pairs."""
    return TimeSeriesRange.model_validate({'metric': {'__name__': name}, 'values': values})


def matrix_handler(request: Request) -> Response:
    """A query_range answer: one point per selected name, at the query's start, valued by database and name."""
    params = parse_qs(request.read().decode())
    db = 'D' if '/victoria-dense/' in str(request.url) else 'A'
    result = [
        {'metric': {'__name__': name}, 'values': [[int(params['start'][0]), f'{db}-{name}']]}
        for name in re.findall(r'__name__="(\w+)"', params['query'][0])
    ]
    return Response(200, json={'status': 'success', 'data': {'resultType': 'matrix', 'result': result}})


@pytest.mark.usefixtures('unrefined')
async def test_ranges_seam(
    vic: victoria.VictoriaClient,
    url: str,
    dense_url: str,
    now: datetime,
    httpx_mock: HTTPXMock,
    monkeypatch: pytest.MonkeyPatch,
):
    # Several name batches over a long-term and a dense part: each series gets both, in order
    monkeypatch.setattr(planner, 'SELECTOR_MAX_NAMES', 1)
    ts = int(now.timestamp())
    vic.cursor = ts - 600
    for db_url in [url, dense_url]:
        httpx_mock.add_callback(
            url=f'{db_url}/api/v1/query_range', method='POST', callback=matrix_handler, is_reusable=True
        )

    result = await vic.ranges(TimeSeriesRangesQuery(fields=['a', 'b'], duration=timedelta(days=1)))
    assert result == [series(name, [[ts - 86400, f'A-{name}'], [ts - 480, f'D-{name}']]) for name in ['a', 'b']]
    assert len(httpx_mock.get_requests()) == 4


async def test_ranges_dense_down(
    vic: victoria.VictoriaClient,
    url: str,
    dense_url: str,
    now: datetime,
    httpx_mock: HTTPXMock,
    caplog: pytest.LogCaptureFixture,
):
    ts = int(now.timestamp())
    vic.cursor = ts - 600
    httpx_mock.add_exception(
        url=f'{dense_url}/api/v1/query_range',
        method='POST',
        exception=httpx.ConnectError('refused'),
        is_reusable=True,
    )
    httpx_mock.add_callback(url=f'{url}/api/v1/query_range', method='POST', callback=matrix_handler, is_reusable=True)

    # With a long-term part in the plan, that part is the answer
    result = await vic.ranges(TimeSeriesRangesQuery(fields=['a'], duration=timedelta(days=1)))
    assert result == [series('a', [[ts - 86400, 'A-a']])]
    assert 'Ranges without the dense database until it answers: ConnectionError' in caplog.text

    # Without one, the long-term database answers everything, at a multiple of its interval
    result = await vic.ranges(TimeSeriesRangesQuery(fields=['a'], duration=timedelta(minutes=10)))
    assert result == [series('a', [[ts - 600, 'A-a']])]
    assert parse_qs(httpx_mock.get_requests()[-1].read().decode())['step'] == ['60s']


async def test_ranges_dense_outage_logged(
    vic: victoria.VictoriaClient,
    url: str,
    dense_url: str,
    httpx_mock: HTTPXMock,
    caplog: pytest.LogCaptureFixture,
):
    # One warning per outage, however many graphs query in the meantime
    caplog.set_level(logging.INFO, logger=victoria.__name__)
    down = True

    def dense_handler(request: Request) -> Response:
        if down:
            raise httpx.ConnectError('refused')
        return matrix_handler(request)

    httpx_mock.add_callback(
        url=f'{dense_url}/api/v1/query_range', method='POST', callback=dense_handler, is_reusable=True
    )
    httpx_mock.add_callback(url=f'{url}/api/v1/query_range', method='POST', callback=matrix_handler, is_reusable=True)
    query = TimeSeriesRangesQuery(fields=['a'], duration=timedelta(minutes=10))

    def messages() -> list[str]:
        return [r.getMessage().split(':')[0] for r in caplog.records if r.name == victoria.__name__]

    await vic.ranges(query)
    await vic.ranges(query)
    assert messages() == ['Ranges without the dense database until it answers']

    down = False
    await vic.ranges(query)
    await vic.ranges(query)
    assert messages() == ['Ranges without the dense database until it answers', 'Ranges from the dense database again']

    down = True
    await vic.ranges(query)
    assert messages() == [
        'Ranges without the dense database until it answers',
        'Ranges from the dense database again',
        'Ranges without the dense database until it answers',
    ]


async def test_ranges_dense_partial_failure(
    vic: victoria.VictoriaClient,
    url: str,
    dense_url: str,
    now: datetime,
    httpx_mock: HTTPXMock,
    monkeypatch: pytest.MonkeyPatch,
):
    # A dense-only plan of which one name batch fails: the long-term database answers all fields
    monkeypatch.setattr(planner, 'SELECTOR_MAX_NAMES', 1)
    ts = int(now.timestamp())

    def dense_handler(request: Request) -> Response:
        if '__name__="b"' in parse_qs(request.read().decode())['query'][0]:
            raise httpx.ReadTimeout('slow')
        return matrix_handler(request)

    httpx_mock.add_callback(
        url=f'{dense_url}/api/v1/query_range', method='POST', callback=dense_handler, is_reusable=True
    )
    httpx_mock.add_callback(url=f'{url}/api/v1/query_range', method='POST', callback=matrix_handler, is_reusable=True)

    result = await vic.ranges(TimeSeriesRangesQuery(fields=['a', 'b'], duration=timedelta(minutes=10)))
    assert result == [series(name, [[ts - 600, f'A-{name}']]) for name in ['a', 'b']]


async def test_ranges_dense_since(
    vic: victoria.VictoriaClient,
    url: str,
    dense_url: str,
    now: datetime,
    httpx_mock: HTTPXMock,
):
    # Before the dense database's samples start, the long-term database answers
    ts = int(now.timestamp())
    vic.cursor = ts - 120
    vic.dense_since = ts - 300
    for db_url in [url, dense_url]:
        httpx_mock.add_callback(
            url=f'{db_url}/api/v1/query_range', method='POST', callback=matrix_handler, is_reusable=True
        )

    result = await vic.ranges(TimeSeriesRangesQuery(fields=['a'], duration=timedelta(minutes=10)))
    assert result == [series('a', [[ts - 600, 'A-a'], [ts - 60, 'D-a']])]


@pytest.mark.usefixtures('unrefined')
async def test_initial_ranges_follow_up(
    vic: victoria.VictoriaClient,
    url: str,
    dense_url: str,
    now: datetime,
    httpx_mock: HTTPXMock,
    monkeypatch: pytest.MonkeyPatch,
):
    # Follow-ups continue after the newest point sent, at the frame's step, capped at 10 s
    monkeypatch.setattr(planner, 'SELECTOR_MAX_NAMES', 1)
    ts = int(now.timestamp())
    vic.cursor = ts - 600
    # Names whose dense batch fails, and databases that have none of the names
    failing: set[str] = set()
    silent: set[str] = set()

    def last_point(request: Request) -> Response:
        """One point per selected name, at the query's last point, valued by database and name.
        Series 'a' ends a step earlier."""
        params = parse_qs(request.read().decode())
        db = 'D' if '/victoria-dense/' in str(request.url) else 'A'
        names = re.findall(r'__name__="(\w+)"', params['query'][0])
        if db == 'D' and failing & set(names):
            raise httpx.ReadTimeout('slow')
        start, end, step = int(params['start'][0]), int(params['end'][0]), int(params['step'][0][:-1])
        last = start + (end - start) // step * step
        result = [
            {'metric': {'__name__': n}, 'values': [[last - step if n == 'a' else last, f'{db}-{n}']]}
            for n in names
            if db not in silent
        ]
        return Response(200, json={'status': 'success', 'data': {'resultType': 'matrix', 'result': result}})

    for db_url in [url, dense_url]:
        httpx_mock.add_callback(
            url=f'{db_url}/api/v1/query_range', method='POST', callback=last_point, is_reusable=True
        )

    def query(duration: str) -> TimeSeriesRangesQuery:
        return TimeSeriesRangesQuery.model_validate({'fields': ['a', 'b'], 'duration': duration})

    # A day at 120 s, from both databases: after the newest point of any series
    _, follow = await vic.initial_ranges(query('1d'))
    assert follow == (ts - 120, 10, ts - 5)
    # Ten minutes at 1 s
    result, follow = await vic.initial_ranges(query('10m'))
    assert [r.values[-1].timestamp for r in result] == [ts - 6, ts - 5]
    assert follow == (ts - 5, 1, ts - 5)

    # One dense batch fails: the long-term part answers for every field, and the follow-ups fill in the rest
    failing = {'b'}
    result, follow = await vic.initial_ranges(query('1d'))
    assert result == [
        series('a', [[ts - 720, 'A-a']]),
        series('b', [[ts - 600, 'A-b']]),
    ]
    assert follow == (ts - 600, 10, ts - 5)

    # The dense database has none of the fields: after the fallback's newest point, at the frame's step
    failing = set()
    silent = {'D'}
    result, follow = await vic.initial_ranges(query('10m'))
    assert result == [
        series('a', [[ts - 120, 'A-a']]),
        series('b', [[ts - 60, 'A-b']]),
    ]
    assert follow == (ts - 60, 1, ts - 5)

    # Nothing sent: after the frame's start
    silent = {'A', 'D'}
    assert await vic.initial_ranges(query('10m')) == ([], (ts - 600, 1, ts - 5))
    # Every field, also without values
    assert await vic.initial_ranges(query('10m'), every_field=True) == (
        [series(n, []) for n in ['a', 'b']],
        (ts - 600, 1, ts - 5),
    )


def stretch(
    first: int,
    last: int,
    *,
    missing: frozenset[tuple[str, str]] = frozenset(),
    fails: Callable[[str, int], bool] = lambda _db, _step: False,
    refused: Callable[[str, int], bool] = lambda _db, _step: False,
) -> Callable[[Request], Response]:
    """A query_range handler for series with samples from first to last: every point whose step holds some,
    valued by database ('A' or 'D') and step. The series of the (database, name) pairs in missing have none;
    a query for which fails(database, step) is true fails, and one for which refused(database, step) is true
    is refused (as the database refuses one that asks for too many points)."""

    def handler(request: Request) -> Response:
        params = parse_qs(request.read().decode())
        db = 'D' if '/victoria-dense/' in str(request.url) else 'A'
        start, end, step = int(params['start'][0]), int(params['end'][0]), int(params['step'][0][:-1])
        if fails(db, step):
            raise httpx.ConnectError('refused')
        if refused(db, step):
            return Response(422, json={'status': 'error', 'errorType': '422', 'error': 'too many points'})
        points = [t for t in range(start, end + 1, step) if first <= t and t - step < last]
        result = [
            {'metric': {'__name__': name}, 'values': [[t, f'{db}{step}'] for t in points]}
            for name in re.findall(r'__name__="(\w+)"', params['query'][0])
            if points and (db, name) not in missing
        ]
        return Response(200, json={'status': 'success', 'data': {'resultType': 'matrix', 'result': result}})

    return handler


# Both databases' query_range
QUERY_RANGE = re.compile(r'.*/api/v1/query_range')


def asked(httpx_mock: HTTPXMock) -> list[tuple[str, int]]:
    """The database and step of each query_range request."""
    return [
        (str(r.url).split('/')[3], int(parse_qs(r.read().decode())['step'][0][:-1])) for r in httpx_mock.get_requests()
    ]


@pytest.mark.parametrize(
    ('duration', 'since', 'until', 'expected'),
    [
        # A week at 660 s with a day of samples, in the long-term database: again at 120 s, where they are
        (7 * DAY, 3 * DAY, 2 * DAY, [('victoria', 660), ('victoria-dense', 660), ('victoria', 120)]),
        # Twelve hours at 43 s with one of samples: again at 4 s
        (12 * HOUR, 3 * HOUR, 2 * HOUR, [('victoria-dense', 43), ('victoria-dense', 4)]),
        # A week with four hours of samples: below 60 s, again from the dense database
        (7 * DAY, DAY + 4 * HOUR, DAY, [('victoria', 660), ('victoria-dense', 660), ('victoria-dense', 16)]),
        # Two years with three days of samples, a year ago
        (730 * DAY, 365 * DAY, 362 * DAY, [('victoria', 63120), ('victoria', 360)]),
        # Samples up to now: again from both
        (
            7 * DAY,
            36 * HOUR,
            0,
            [('victoria', 660), ('victoria-dense', 660), ('victoria', 180), ('victoria-dense', 180)],
        ),
        # A day full of samples: once
        (DAY, 2 * DAY, 0, [('victoria', 120), ('victoria-dense', 120)]),
    ],
)
async def test_ranges_refined(
    vic: victoria.VictoriaClient,
    now: datetime,
    httpx_mock: HTTPXMock,
    duration: int,
    since: int,
    until: int,
    expected: list[tuple[str, int]],
):
    # A mostly empty answer is asked again, finer, where it has points
    ts = int(now.timestamp())
    vic.cursor = ts - 600
    httpx_mock.add_callback(url=QUERY_RANGE, method='POST', callback=stretch(ts - since, ts - until), is_reusable=True)

    query = TimeSeriesRangesQuery(fields=['a', 'b'], duration=timedelta(seconds=duration))
    result, follow = await vic.initial_ranges(query)
    assert asked(httpx_mock) == expected

    # The last answer: every point whose step holds samples, from the first one or the start
    # up to the last one or the end (query_latency before now)
    step = expected[-1][1]
    for r in result:
        timestamps = [int(v.timestamp) for v in r.values]
        first, last = timestamps[0], timestamps[-1]
        assert timestamps == list(range(first, last + 1, step))
        assert first - step < ts - since <= first or first <= ts - duration < first + step
        assert last - step < ts - until <= last or last <= ts - 5 < last + step
        assert {v.value[1:] for v in r.values} == {str(step)}
    # Follow-ups continue after it
    assert follow == (max(int(r.values[-1].timestamp) for r in result), 10, ts - 5)


async def test_ranges_refined_fails(
    vic: victoria.VictoriaClient,
    now: datetime,
    httpx_mock: HTTPXMock,
    caplog: pytest.LogCaptureFixture,
):
    # The first answer stands when the finer one fails, also in part
    ts = int(now.timestamp())
    vic.cursor = ts - 600
    handler = stretch(ts - DAY, ts)

    def answer(request: Request) -> Response:
        return handler(request)

    httpx_mock.add_callback(url=QUERY_RANGE, method='POST', callback=answer, is_reusable=True)

    def values(result: list[TimeSeriesRange]) -> set[str]:
        return {v.value for r in result for v in r.values}

    def query(duration: str) -> TimeSeriesRangesQuery:
        return TimeSeriesRangesQuery.model_validate({'fields': ['a', 'b'], 'duration': duration})

    # The dense part of a finer plan fails: the first answer, from both
    handler = stretch(ts - 36 * HOUR, ts, fails=lambda db, step: db == 'D' and step == 180)
    assert values(await vic.ranges(query('7d'))) == {'A660', 'D660'}
    assert 'Ranges without the dense database until it answers: ConnectionError' in caplog.text

    # The long-term database fails the finer plan, or refuses it
    handler = stretch(ts - 3 * DAY, ts - 2 * DAY, fails=lambda db, step: db == 'A' and step == 120)
    assert values(await vic.ranges(query('7d'))) == {'A660'}
    assert 'Ranges at the first step, the finer query failed: ConnectionError' in caplog.text
    handler = stretch(ts - 3 * DAY, ts - 2 * DAY, refused=lambda db, step: db == 'A' and step == 120)
    assert values(await vic.ranges(query('7d'))) == {'A660'}
    assert 'Ranges at the first step, the finer query failed: HTTPStatusError' in caplog.text

    # The dense database lacks a series: it keeps the long-term database's points
    handler = stretch(ts - DAY - 4 * HOUR, ts - DAY, missing=frozenset({('D', 'b')}))
    result = await vic.ranges(query('7d'))
    assert [values([r]) for r in result] == [{'D16'}, {'A660'}]


async def test_ranges_refined_degraded(
    vic: victoria.VictoriaClient,
    now: datetime,
    httpx_mock: HTTPXMock,
):
    ts = int(now.timestamp())
    vic.cursor = ts - 600
    handler = stretch(ts - DAY, ts)

    def answer(request: Request) -> Response:
        return handler(request)

    httpx_mock.add_callback(url=QUERY_RANGE, method='POST', callback=answer, is_reusable=True)

    async def ranges(duration: timedelta) -> tuple[set[str], list[tuple[str, int]]]:
        """The values of the answer, and the queries it took."""
        before = len(httpx_mock.get_requests())
        result = await vic.ranges(TimeSeriesRangesQuery(fields=['a'], duration=duration))
        return {v.value for r in result for v in r.values}, asked(httpx_mock)[before:]

    # The dense database has none of the fields: the fallback, at the long-term database's interval, not asked again
    handler = stretch(ts - 3 * HOUR, ts - 2 * HOUR, missing=frozenset({('D', 'a')}))
    assert await ranges(timedelta(hours=12)) == ({'A60'}, [('victoria-dense', 43), ('victoria', 60)])

    # The dense database fails: the long-term part of the plan, not asked again from dense, also in part
    handler = stretch(ts - 36 * HOUR, ts, fails=lambda db, _step: db == 'D')
    assert not vic._dense_reads_failing
    assert await ranges(timedelta(days=7)) == ({'A660'}, [('victoria', 660), ('victoria-dense', 660)])
    # Asked again when the finer plan does not read dense
    handler = stretch(ts - 3 * DAY, ts - 2 * DAY, fails=lambda db, _step: db == 'D')
    assert await ranges(timedelta(days=7)) == (
        {'A120'},
        [('victoria', 660), ('victoria-dense', 660), ('victoria', 120)],
    )
    handler = stretch(ts - DAY - 4 * HOUR, ts - DAY, fails=lambda db, _step: db == 'D')
    assert await ranges(timedelta(days=7)) == ({'A660'}, [('victoria', 660), ('victoria-dense', 660)])

    # Dense reads failed before: a first plan without dense is not asked again from dense either, until they work
    handler = stretch(ts - 7 * DAY - 4 * HOUR, ts - 7 * DAY)
    assert vic._dense_reads_failing
    assert await ranges(timedelta(days=180)) == ({'A15600'}, [('victoria', 15600)])
    vic._dense_reads_failing = False
    assert await ranges(timedelta(days=180)) == ({'D32'}, [('victoria', 15600), ('victoria-dense', 32)])


async def test_follow_up_ranges(
    vic: victoria.VictoriaClient,
    dense_url: str,
    now: datetime,
    httpx_mock: HTTPXMock,
    caplog: pytest.LogCaptureFixture,
):
    ts = int(now.timestamp())
    httpx_mock.add_callback(url=f'{dense_url}/api/v1/query_range', method='POST', callback=matrix_handler)

    # Not due: no query
    follow = planner.FollowUp(ts - 14, 10, ts - 14)
    assert await vic.follow_up_ranges(['a'], follow) == ([], follow)
    # The clock went back up to a minute: wait for it to catch up
    follow = planner.FollowUp(ts - 14, 10, ts + 55)
    assert await vic.follow_up_ranges(['a'], follow) == ([], follow)
    # Further: start over
    assert 'The clock went back' not in caplog.text
    assert await vic.follow_up_ranges(['a'], planner.FollowUp(ts - 14, 10, ts + 56)) == ([], None)
    assert 'The clock went back: live ranges start over' in caplog.text
    assert not httpx_mock.get_requests()

    # The points after the last one, up to query_latency before now, ending on the last one
    follow = planner.FollowUp(ts - 37, 10, ts - 37)
    result, follow = await vic.follow_up_ranges(['a', 'b', 'a'], follow)
    assert result == [series(n, [[ts - 27, f'D-{n}']]) for n in ['a', 'b']]
    assert follow == (ts - 7, 10, ts - 7)
    [request] = httpx_mock.get_requests()
    params = parse_qs(request.read().decode())
    assert params['query'] == ['avg_over_time({__name__="a" or __name__="b"}[10s]) keep_metric_names']
    assert (params['start'], params['end'], params['step']) == ([str(ts - 27)], [str(ts - 7)], ['10s'])
    assert params['latency_offset'] == ['5.0']
    # The start need not be on the grid of the step: the database must not round it
    assert params['nocache'] == ['1']


async def test_follow_up_ranges_dense(
    vic: victoria.VictoriaClient,
    dense_url: str,
    now: datetime,
    httpx_mock: HTTPXMock,
    caplog: pytest.LogCaptureFixture,
):
    # From the dense database, and never from the long-term one
    ts = int(now.timestamp())
    follow = planner.FollowUp(ts - 37, 10, ts - 37)
    down = True

    def dense_handler(request: Request) -> Response:
        if down:
            raise httpx.ConnectError('refused')
        return matrix_handler(request)

    httpx_mock.add_callback(
        url=f'{dense_url}/api/v1/query_range', method='POST', callback=dense_handler, is_reusable=True
    )

    # While it fails: nothing, and the next follow-up continues from the same point
    assert await vic.follow_up_ranges(['a'], follow) == ([], follow)
    assert 'Ranges without the dense database until it answers' in caplog.text

    down = False
    assert await vic.follow_up_ranges(['a'], follow) == (
        [series('a', [[ts - 27, 'D-a']])],
        (ts - 7, 10, ts - 7),
    )
    assert len(httpx_mock.get_requests()) == 2


async def test_csv_dense_since(
    vic: victoria.VictoriaClient,
    url: str,
    dense_url: str,
    now: datetime,
    httpx_mock: HTTPXMock,
):
    # The switch is on the interval's grid, at or after where the dense database's samples start
    ts = int(now.timestamp())
    vic.dense_since = ts - 270
    for db_url in [url, dense_url]:
        httpx_mock.add_response(url=f'{db_url}/api/v1/export', method='POST', text='', is_reusable=True)

    lines = [
        line async for line in vic.csv(TimeSeriesCsvQuery(fields=['a'], duration=timedelta(hours=1), precision='ms'))
    ]
    assert lines == ['time,a']
    exports = [(str(r.url).split('/')[3], parse_qs(r.read().decode())) for r in httpx_mock.get_requests()]
    assert [(db, int(p['start'][0]), int(p['end'][0])) for db, p in exports] == [
        ('victoria', ts - 3600, ts - 240),
        ('victoria-dense', ts - 240, ts - 5),
    ]


async def test_ranges_archive_down(vic: victoria.VictoriaClient, url: str, dense_url: str, httpx_mock: HTTPXMock):
    # The long-term database has no stand-in
    httpx_mock.add_exception(url=f'{url}/api/v1/query_range', method='POST', exception=httpx.ConnectError('refused'))
    httpx_mock.add_callback(
        url=f'{dense_url}/api/v1/query_range', method='POST', callback=matrix_handler, is_reusable=True
    )
    vic.cursor = int(utils.now().timestamp()) - 600
    with pytest.raises(ConnectionError, match=f'{url}/: ConnectError'):
        await vic.ranges(TimeSeriesRangesQuery(fields=['a'], duration=timedelta(days=1)))


async def test_csv_unreachable(vic: victoria.VictoriaClient, dense_url: str, httpx_mock: HTTPXMock):
    httpx_mock.add_exception(url=f'{dense_url}/api/v1/export', method='POST', exception=httpx.ConnectError('refused'))
    args = TimeSeriesCsvQuery(fields=['a'], precision='ISO8601')
    with pytest.raises(ConnectionError, match=f'{dense_url}/: ConnectError'):
        async for _ in vic.csv(args):
            pass


async def test_csv(vic: victoria.VictoriaClient, dense_url: str, httpx_mock: HTTPXMock):
    # Each 6 h window of the default 1 d gets the same response from the dense database: rows are not repeated
    lines = [
        (
            '{"metric":{"__name__":"sparkey/HERMS BK PWM/setting"},'
            '"values":[0,0,0,0,0,0,0],'
            '"timestamps":[1626368070381,1626368075435,1626368080487,1626368085534,'
            '1626368090630,1626368095687,1626368100749]}'
        ),
        (
            '{"metric":{"__name__":"sparkey/HERMS BK PWM/setting"},'
            '"values":[0,0,0,0],'
            '"timestamps":[1626368105840,1626368110891,1626368115940,1626368121034]}'
        ),
        (
            '{"metric":{"__name__":"spock/actuator-1/value"},'
            '"values":[40,40,40,40,40,40,40,40,40,40,40,40,40],'
            '"timestamps":[1626368060379,1626368060380,1626368070380,1626368078080,1626368083130,1626368088178,'
            '1626368093272,1626368098328,1626368103383,1626368108480,1626368113533,1626368118579,1626368123669]}'
        ),
        (
            '{"metric":{"__name__":"spock/pin-actuator-1/state"},'
            '"values":[0,0,0,0,0,0,0,0,0,0,0],'
            '"timestamps":[1626368070380,1626368078080,1626368083130,1626368088178,'
            '1626368093272,1626368098328,1626368103383,1626368108480,1626368113533,1626368118579,1626368123669]}'
        ),
    ]
    httpx_mock.add_response(url=f'{dense_url}/api/v1/export', method='POST', is_reusable=True, text='\n'.join(lines))

    args = TimeSeriesCsvQuery(
        fields=['sparkey/HERMS BK PWM/setting', 'spock/pin-actuator-1/state', 'spock/actuator-1/value'],
        precision='ISO8601',
    )

    result = [line async for line in vic.csv(args)]
    assert len(result) == 25  # headers, 13 from sparkey, 11 from spock
    assert result[0] == ','.join(['time', *args.fields])

    # line 1: values from spock
    line = result[1].split(',')
    assert ciso8601.parse_datetime(line[0])
    assert line[1:] == ['', '', '40']

    # line 4: values from sparkey
    line = result[4].split(',')
    assert ciso8601.parse_datetime(line[0])
    assert line[1:] == ['0', '', '']

    # Assert that result is sorted by time
    timestamps = [v[0] for v in [ln.split(',') for ln in result[1:]]]
    assert timestamps == sorted(timestamps)

    # One export per window, matching the fields exactly
    requests = httpx_mock.get_requests()
    assert len(requests) == 4
    params = [parse_qs(r.read().decode()) for r in requests]
    assert params[0]['match[]'] == [
        (
            '{__name__="sparkey/HERMS BK PWM/setting" or __name__="spock/pin-actuator-1/state"'
            ' or __name__="spock/actuator-1/value"}'
        )
    ]
    assert [int(p['end'][0]) - int(p['start'][0]) for p in params][:3] == [6 * 3600] * 3
    assert all(params[i]['end'] == params[i + 1]['start'] for i in range(3))


async def test_csv_rejected(vic: victoria.VictoriaClient, dense_url: str, httpx_mock: HTTPXMock):
    httpx_mock.add_response(url=f'{dense_url}/api/v1/export', method='POST', status_code=422, text='bad query')
    args = TimeSeriesCsvQuery(fields=['a'], duration=timedelta(hours=1), precision='ISO8601')
    with pytest.raises(httpx.HTTPStatusError):
        async for _ in vic.csv(args):
            pass


async def test_write(vic: victoria.VictoriaClient, now: datetime, written: list[str]):
    await vic.write(HistoryEvent.model_validate({'key': 'service', 'data': {'f1': 1, 'f2': 'invalid'}}))
    await vic.write(HistoryEvent(key='service', data={}))

    assert written == ['service f1=1.0']

    args = TimeSeriesMetricsQuery(fields=['service/f1'])
    assert await vic.metrics(args) == [
        TimeSeriesMetric(
            metric='service/f1',
            value=1.0,
            timestamp=now,
        ),
    ]

    await vic.write(HistoryEvent(key='service', data={'f1': 2, 'f2': 3}))
    assert written == [
        'service f1=1.0',
        'service f1=2.0,f2=3.0',
    ]


async def test_write_escaping(vic: victoria.VictoriaClient, now: datetime, written: list[str]):
    await vic.write(
        HistoryEvent.model_validate(
            {
                'key': 'my "spark",1\\',
                'data': {
                    'block, one': {'value=x': 1},
                    'back\\slash\\': 2,
                },
            }
        )
    )
    # Backslash, comma and space in the measurement.
    # Backslash, comma, equals sign and space in field keys.
    assert written == ['my\\ "spark"\\,1\\\\ back\\\\slash\\\\=2.0,block\\,\\ one/value\\=x=1.0']

    # The metrics cache uses the names as published
    args = TimeSeriesMetricsQuery(fields=['my "spark",1\\/block, one/value=x'])
    assert await vic.metrics(args) == [
        TimeSeriesMetric(metric='my "spark",1\\/block, one/value=x', value=1, timestamp=now),
    ]


async def test_write_timestamp(
    vic: victoria.VictoriaClient, now: datetime, written: list[str], caplog: pytest.LogCaptureFixture
):
    now_ms = utils.to_millis(now)
    args = TimeSeriesMetricsQuery(fields=['service/f1'])

    # Accepted within 10 s either way
    await vic.write(HistoryEvent(key='service', data={'f1': 1}, timestamp=now_ms - 10_000))
    await vic.write(HistoryEvent(key='service', data={'f1': 2}, timestamp=now_ms + 10_000))
    assert await vic.metrics(args) == [
        TimeSeriesMetric(metric='service/f1', value=2, timestamp=now + timedelta(seconds=10)),
    ]

    # Further off, the database stamps arrival, and the cache uses our time
    await vic.write(HistoryEvent(key='service', data={'f1': 3}, timestamp=now_ms - 10_001))
    assert await vic.metrics(args) == [
        TimeSeriesMetric(metric='service/f1', value=3, timestamp=now),
    ]

    # The warning comes once per key
    await vic.write(HistoryEvent(key='service', data={'f1': 4}, timestamp=now_ms))
    await vic.write(HistoryEvent(key='service', data={'f1': 5}, timestamp=0))
    await vic.write(HistoryEvent(key='other', data={'f1': 6}, timestamp=now_ms + 20_000))

    assert written == [
        f'service f1=1.0 {now_ms - 10_000}',
        f'service f1=2.0 {now_ms + 10_000}',
        'service f1=3.0',
        f'service f1=4.0 {now_ms}',
        'service f1=5.0',
        'other f1=6.0',
    ]
    assert [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING] == [
        'service: event timestamp is -10.0s off, using arrival time',
        'other: event timestamp is +20.0s off, using arrival time',
    ]


async def test_write_dense(vic: victoria.VictoriaClient, dense_url: str, httpx_mock: HTTPXMock):
    # Raw samples go to the dense database only
    httpx_mock.add_response(url=f'{dense_url}/write?precision=ms', method='POST')
    await vic.write(HistoryEvent(key='service', data={'f1': 1}))
    assert [str(r.url) for r in httpx_mock.get_requests()] == [f'{dense_url}/write?precision=ms']


async def test_write_exc(
    vic: victoria.VictoriaClient, dense_url: str, httpx_mock: HTTPXMock, caplog: pytest.LogCaptureFixture
):
    httpx_mock.add_exception(
        url=f'{dense_url}/write?precision=ms', method='POST', exception=RuntimeError('dummy error')
    )

    # Write errors are logged with the database, and swallowed
    await vic.write(HistoryEvent(key='service', data={'f1': 1}))
    assert f'{dense_url}/: write failed: RuntimeError(dummy error)' in caplog.text

    # Once, and when writes work again, which also makes the next outage show
    caplog.set_level(logging.INFO, logger=victoria.__name__)
    httpx_mock.add_exception(
        url=f'{dense_url}/write?precision=ms', method='POST', exception=RuntimeError('dummy error')
    )
    httpx_mock.add_response(url=f'{dense_url}/write?precision=ms', method='POST', is_reusable=True)
    await vic.write(HistoryEvent(key='service', data={'f1': 2}))
    await vic.write(HistoryEvent(key='service', data={'f1': 3}))
    await vic.write(HistoryEvent(key='service', data={'f1': 4}))
    httpx_mock.add_exception(
        url=f'{dense_url}/write?precision=ms', method='POST', exception=RuntimeError('dummy error')
    )
    await vic.write(HistoryEvent(key='service', data={'f1': 5}))
    assert [r.getMessage() for r in caplog.records if r.name == victoria.__name__] == [
        f'{dense_url}/: write failed: RuntimeError(dummy error)',
        f'{dense_url}/: writes work again',
        f'{dense_url}/: write failed: RuntimeError(dummy error)',
    ]


async def test_write_rejected(
    vic: victoria.VictoriaClient, dense_url: str, httpx_mock: HTTPXMock, caplog: pytest.LogCaptureFixture
):
    httpx_mock.add_response(
        url=f'{dense_url}/write?precision=ms', method='POST', status_code=400, text='cannot parse line'
    )

    # Rejected writes are logged with the database and its reason, and swallowed
    await vic.write(HistoryEvent(key='service', data={'f1': 1}))
    assert f'{dense_url}/: write failed: HTTPStatusError' in caplog.text
    assert 'cannot parse line' in caplog.text

    # A rejected line is a problem with its data, not an outage: the next accepted write does not say writes work again
    caplog.set_level(logging.INFO, logger=victoria.__name__)
    httpx_mock.add_response(url=f'{dense_url}/write?precision=ms', method='POST')
    await vic.write(HistoryEvent(key='service', data={'f1': 2}))
    assert 'writes work again' not in caplog.text

    # An error of the database is an outage, and the next accepted write says it ended
    httpx_mock.add_response(url=f'{dense_url}/write?precision=ms', method='POST', status_code=503, text='overloaded')
    httpx_mock.add_response(url=f'{dense_url}/write?precision=ms', method='POST')
    await vic.write(HistoryEvent(key='service', data={'f1': 3}))
    await vic.write(HistoryEvent(key='service', data={'f1': 4}))
    assert f'{dense_url}/: writes work again' in caplog.text


async def test_query_rejected(vic: victoria.VictoriaClient, url: str, dense_url: str, httpx_mock: HTTPXMock):
    httpx_mock.add_response(url=f'{url}/api/v1/series', method='POST', status_code=422, text='bad query')
    httpx_mock.add_response(url=f'{dense_url}/api/v1/series', method='POST', json={'status': 'success', 'data': []})

    with pytest.raises(httpx.HTTPStatusError):
        await vic.fields(TimeSeriesFieldsQuery())


async def test_close_dense(
    vic: victoria.VictoriaClient, url: str, mocker: MockerFixture, caplog: pytest.LogCaptureFixture
):
    # A failing client does not keep the other open
    mocker.patch.object(vic._archive, 'aclose', side_effect=RuntimeError('dummy'))
    await vic.close()
    assert vic._database('dense').is_closed
    assert f'{url}/: close failed: RuntimeError(dummy)' in caplog.text


async def test_lifespan(vic: victoria.VictoriaClient, mocker: MockerFixture):
    m_close = mocker.patch.object(vic, 'close', autospec=True)
    async with victoria.lifespan():
        m_close.assert_not_awaited()
    m_close.assert_awaited_once()
