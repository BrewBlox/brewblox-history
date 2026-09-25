"""
Tests brewblox_history.victoria
"""

import logging
import re
from datetime import datetime, timedelta, timezone
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
    dt = datetime(2021, 7, 15, 19, tzinfo=timezone.utc)
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
def dense_vic(config: ServiceConfig) -> victoria.VictoriaClient:
    config.dense_enabled = True
    victoria.setup()
    return victoria.CV.get()


@pytest.fixture
def written(url: str, httpx_mock: HTTPXMock) -> list[str]:
    written = []

    async def handler(request: Request) -> Response:
        written.append(request.read().decode())
        return Response(200)

    httpx_mock.add_callback(url=f'{url}/write?precision=ms', method='POST', callback=handler, is_reusable=True)
    return written


async def test_ping(vic: victoria.VictoriaClient, url: str, httpx_mock: HTTPXMock):
    httpx_mock.add_response(url=f'{url}/health', method='GET', text='OK')
    await vic.ping()

    httpx_mock.add_response(url=f'{url}/health', method='GET', text='NOK')
    with pytest.raises(ConnectionError):
        await vic.ping()


async def test_ping_dense(dense_vic: victoria.VictoriaClient, url: str, dense_url: str, httpx_mock: HTTPXMock):
    # Both databases must be healthy
    httpx_mock.add_response(url=f'{url}/health', method='GET', text='OK', is_reusable=True)
    httpx_mock.add_response(url=f'{dense_url}/health', method='GET', text='OK')
    await dense_vic.ping()

    # Errors name the database
    httpx_mock.add_response(url=f'{dense_url}/health', method='GET', text='NOK')
    with pytest.raises(ConnectionError, match=f'{dense_url}/: ping returned warning: "NOK"'):
        await dense_vic.ping()

    httpx_mock.add_exception(url=f'{dense_url}/health', method='GET', exception=httpx.ConnectError('refused'))
    with pytest.raises(ConnectionError, match=f'{dense_url}/: ConnectError'):
        await dense_vic.ping()


async def test_ping_dense_both_down(
    dense_vic: victoria.VictoriaClient, url: str, dense_url: str, httpx_mock: HTTPXMock
):
    # Every failing database is reported
    httpx_mock.add_response(url=f'{url}/health', method='GET', text='NOK')
    httpx_mock.add_exception(url=f'{dense_url}/health', method='GET', exception=httpx.ConnectError('refused'))
    with pytest.raises(ConnectionError) as info:
        await dense_vic.ping()
    assert f'{url}/: ping returned warning: "NOK"' in str(info.value)
    assert f'{dense_url}/: ConnectError(refused)' in str(info.value)


async def test_fields(vic: victoria.VictoriaClient, url: str, httpx_mock: HTTPXMock):
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

    args = TimeSeriesFieldsQuery(duration='1d')
    assert await vic.fields(args) == [
        'sparkey/HERMS HLT PID/inputValue[degC]',
        'sparkey/HERMS MT PID/integralReset',
        'spock/actuator-1/value',
        'spock/setpoint-sensor-pair-2/setting[degC]',
    ]


async def test_fields_dense(dense_vic: victoria.VictoriaClient, url: str, dense_url: str, httpx_mock: HTTPXMock):
    # Series from either database: new ones may not be in the long-term database yet
    for db_url, names in [(url, ['b', 'a']), (dense_url, ['c', 'b'])]:
        httpx_mock.add_response(
            url=f'{db_url}/api/v1/series',
            method='POST',
            json={'status': 'success', 'data': [{'__name__': n} for n in names]},
        )

    assert await dense_vic.fields(TimeSeriesFieldsQuery(duration='1d')) == ['a', 'b', 'c']


@pytest.mark.parametrize(
    'failure, logged',
    [
        ({'exception': httpx.ConnectError('refused')}, 'ConnectionError({dense_url}/: ConnectError(refused))'),
        ({'status_code': 503, 'text': 'too many requests'}, "HTTPStatusError(Server error '503 Service Unavailable'"),
    ],
)
async def test_fields_dense_down(
    dense_vic: victoria.VictoriaClient,
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

    # Without the dense database, the long-term database's fields are used
    httpx_mock.add_response(
        url=f'{url}/api/v1/series',
        method='POST',
        json={'status': 'success', 'data': [{'__name__': 'a'}]},
    )
    assert await dense_vic.fields(TimeSeriesFieldsQuery(duration='1d')) == ['a']
    assert 'Fields from the long-term database only: ' + logged.format(dense_url=dense_url) in caplog.text

    # Without the long-term database, the request fails
    httpx_mock.add_exception(url=f'{url}/api/v1/series', method='POST', exception=httpx.ConnectError('refused'))
    with pytest.raises(ConnectionError, match=f'{url}/: ConnectError'):
        await dense_vic.fields(TimeSeriesFieldsQuery(duration='1d'))


async def test_metrics(vic: victoria.VictoriaClient, now: datetime, written: list[str]):
    args = TimeSeriesMetricsQuery(fields=['service/f1', 'service/f2'])

    # No values cached yet
    assert await vic.metrics(args) == []

    # Don't return invalid values
    await vic.write(HistoryEvent(key='service', data={'f1': 1, 'f2': 'invalid'}))
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


async def test_ranges(vic: victoria.VictoriaClient, url: str, httpx_mock: HTTPXMock):
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
        url=f'{url}/api/v1/query_range',
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
    args = TimeSeriesRangesQuery(fields=['f1', 'f2', 'f3', 'f1'], duration='1h')
    assert await vic.ranges(args) == [TimeSeriesRange(**result('f1')), TimeSeriesRange(**result('f2'))]

    # One request for all fields, which ends query_latency before now and passes it as latency offset
    [request] = httpx_mock.get_requests()
    params = parse_qs(request.read().decode())
    assert params['query'] == [
        'avg_over_time({__name__="f1" or __name__="f2" or __name__="f3"}[10s]) keep_metric_names'
    ]
    assert params['step'] == ['10s']
    assert params['latency_offset'] == ['3.0']
    # The start is on the grid of the step, at most a step before now - 1h
    assert int(params['start'][0]) % 10 == 0
    assert int(params['end'][0]) - int(params['start'][0]) in range(3597, 3597 + 10)


def matrix_handler(request: Request) -> Response:
    """A query_range answer: one point per selected name, at the query's start, valued by database and name."""
    params = parse_qs(request.read().decode())
    db = 'D' if '/victoria-dense/' in str(request.url) else 'A'
    result = [
        {'metric': {'__name__': name}, 'values': [[int(params['start'][0]), f'{db}-{name}']]}
        for name in re.findall(r'__name__="(\w+)"', params['query'][0])
    ]
    return Response(200, json={'status': 'success', 'data': {'resultType': 'matrix', 'result': result}})


async def test_ranges_seam(
    dense_vic: victoria.VictoriaClient,
    url: str,
    dense_url: str,
    now: datetime,
    httpx_mock: HTTPXMock,
    monkeypatch: pytest.MonkeyPatch,
):
    # Several name batches over a long-term and a dense part: each series gets both, in order
    monkeypatch.setattr(planner, 'SELECTOR_MAX_NAMES', 1)
    ts = int(now.timestamp())
    dense_vic.cursor = ts - 600
    for db_url in [url, dense_url]:
        httpx_mock.add_callback(
            url=f'{db_url}/api/v1/query_range', method='POST', callback=matrix_handler, is_reusable=True
        )

    result = await dense_vic.ranges(TimeSeriesRangesQuery(fields=['a', 'b'], duration='1d'))
    assert result == [
        TimeSeriesRange(metric={'__name__': name}, values=[[ts - 86400, f'A-{name}'], [ts - 480, f'D-{name}']])
        for name in ['a', 'b']
    ]
    assert len(httpx_mock.get_requests()) == 4


async def test_ranges_dense_down(
    dense_vic: victoria.VictoriaClient,
    url: str,
    dense_url: str,
    now: datetime,
    httpx_mock: HTTPXMock,
    caplog: pytest.LogCaptureFixture,
):
    ts = int(now.timestamp())
    dense_vic.cursor = ts - 600
    httpx_mock.add_exception(
        url=f'{dense_url}/api/v1/query_range',
        method='POST',
        exception=httpx.ConnectError('refused'),
        is_reusable=True,
    )
    httpx_mock.add_callback(url=f'{url}/api/v1/query_range', method='POST', callback=matrix_handler, is_reusable=True)

    # With a long-term part in the plan, that part is the answer
    result = await dense_vic.ranges(TimeSeriesRangesQuery(fields=['a'], duration='1d'))
    assert result == [TimeSeriesRange(metric={'__name__': 'a'}, values=[[ts - 86400, 'A-a']])]
    assert 'Ranges without the dense database until it answers: ConnectionError' in caplog.text

    # Without one, the long-term database answers everything, at a multiple of its interval
    result = await dense_vic.ranges(TimeSeriesRangesQuery(fields=['a'], duration='10m'))
    assert result == [TimeSeriesRange(metric={'__name__': 'a'}, values=[[ts - 600, 'A-a']])]
    assert parse_qs(httpx_mock.get_requests()[-1].read().decode())['step'] == ['60s']


async def test_ranges_dense_outage_logged(
    dense_vic: victoria.VictoriaClient,
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
    query = TimeSeriesRangesQuery(fields=['a'], duration='10m')

    def messages() -> list[str]:
        return [r.getMessage().split(':')[0] for r in caplog.records if r.name == victoria.__name__]

    await dense_vic.ranges(query)
    await dense_vic.ranges(query)
    assert messages() == ['Ranges without the dense database until it answers']

    down = False
    await dense_vic.ranges(query)
    await dense_vic.ranges(query)
    assert messages() == ['Ranges without the dense database until it answers', 'Ranges from the dense database again']

    down = True
    await dense_vic.ranges(query)
    assert messages() == [
        'Ranges without the dense database until it answers',
        'Ranges from the dense database again',
        'Ranges without the dense database until it answers',
    ]


async def test_ranges_dense_partial_failure(
    dense_vic: victoria.VictoriaClient,
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

    result = await dense_vic.ranges(TimeSeriesRangesQuery(fields=['a', 'b'], duration='10m'))
    assert result == [
        TimeSeriesRange(metric={'__name__': name}, values=[[ts - 600, f'A-{name}']]) for name in ['a', 'b']
    ]


async def test_ranges_dense_since(
    dense_vic: victoria.VictoriaClient,
    url: str,
    dense_url: str,
    now: datetime,
    httpx_mock: HTTPXMock,
):
    # Before the dense database's samples start, the long-term database answers
    ts = int(now.timestamp())
    dense_vic.cursor = ts - 120
    dense_vic.dense_since = ts - 300
    for db_url in [url, dense_url]:
        httpx_mock.add_callback(
            url=f'{db_url}/api/v1/query_range', method='POST', callback=matrix_handler, is_reusable=True
        )

    result = await dense_vic.ranges(TimeSeriesRangesQuery(fields=['a'], duration='10m'))
    assert result == [TimeSeriesRange(metric={'__name__': 'a'}, values=[[ts - 600, 'A-a'], [ts - 60, 'D-a']])]


async def test_csv_dense_since(
    dense_vic: victoria.VictoriaClient,
    url: str,
    dense_url: str,
    now: datetime,
    httpx_mock: HTTPXMock,
):
    # The switch is on the interval's grid, at or after where the dense database's samples start
    ts = int(now.timestamp())
    dense_vic.dense_since = ts - 270
    for db_url in [url, dense_url]:
        httpx_mock.add_response(url=f'{db_url}/api/v1/export', method='POST', text='', is_reusable=True)

    lines = [line async for line in dense_vic.csv(TimeSeriesCsvQuery(fields=['a'], duration='1h', precision='ms'))]
    assert lines == ['time,a']
    exports = [(str(r.url).split('/')[3], parse_qs(r.read().decode())) for r in httpx_mock.get_requests()]
    assert [(db, int(p['start'][0]), int(p['end'][0])) for db, p in exports] == [
        ('victoria', ts - 3600, ts - 240),
        ('victoria-dense', ts - 240, ts - 3),
    ]


async def test_ranges_archive_down(dense_vic: victoria.VictoriaClient, url: str, dense_url: str, httpx_mock: HTTPXMock):
    # The long-term database has no stand-in
    httpx_mock.add_exception(url=f'{url}/api/v1/query_range', method='POST', exception=httpx.ConnectError('refused'))
    httpx_mock.add_callback(
        url=f'{dense_url}/api/v1/query_range', method='POST', callback=matrix_handler, is_reusable=True
    )
    dense_vic.cursor = int(utils.now().timestamp()) - 600
    with pytest.raises(ConnectionError, match=f'{url}/: ConnectError'):
        await dense_vic.ranges(TimeSeriesRangesQuery(fields=['a'], duration='1d'))


async def test_csv_unreachable(vic: victoria.VictoriaClient, url: str, httpx_mock: HTTPXMock):
    httpx_mock.add_exception(url=f'{url}/api/v1/export', method='POST', exception=httpx.ConnectError('refused'))
    args = TimeSeriesCsvQuery(fields=['a'], precision='ISO8601')
    with pytest.raises(ConnectionError, match=f'{url}/: ConnectError'):
        async for _ in vic.csv(args):
            pass


async def test_csv(vic: victoria.VictoriaClient, url: str, httpx_mock: HTTPXMock):
    # Each 6 h window of the default 1 d gets the same response: rows are not repeated
    httpx_mock.add_response(
        url=f'{url}/api/v1/export',
        method='POST',
        is_reusable=True,
        text='\n'.join(
            [
                '{"metric":{"__name__":"sparkey/HERMS BK PWM/setting"},'
                + '"values":[0,0,0,0,0,0,0],'
                + '"timestamps":[1626368070381,1626368075435,1626368080487,1626368085534,'
                + '1626368090630,1626368095687,1626368100749]}',
                '{"metric":{"__name__":"sparkey/HERMS BK PWM/setting"},'
                + '"values":[0,0,0,0],'
                + '"timestamps":[1626368105840,1626368110891,1626368115940,1626368121034]}',
                '{"metric":{"__name__":"spock/actuator-1/value"},'
                + '"values":[40,40,40,40,40,40,40,40,40,40,40,40,40],'
                + '"timestamps":[1626368060379,1626368060380,1626368070380,1626368078080,1626368083130,1626368088178,'
                + '1626368093272,1626368098328,1626368103383,1626368108480,1626368113533,1626368118579,1626368123669]}',
                '{"metric":{"__name__":"spock/pin-actuator-1/state"},'
                + '"values":[0,0,0,0,0,0,0,0,0,0,0],'
                + '"timestamps":[1626368070380,1626368078080,1626368083130,1626368088178,'
                + '1626368093272,1626368098328,1626368103383,1626368108480,1626368113533,1626368118579,1626368123669]}',
            ]
        ),
    )

    args = TimeSeriesCsvQuery(
        fields=['sparkey/HERMS BK PWM/setting', 'spock/pin-actuator-1/state', 'spock/actuator-1/value'],
        precision='ISO8601',
    )

    result = []
    async for line in vic.csv(args):
        result.append(line)
    assert len(result) == 25  # headers, 13 from sparkey, 11 from spock
    assert result[0] == ','.join(['time'] + args.fields)

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
        '{__name__="sparkey/HERMS BK PWM/setting" or __name__="spock/pin-actuator-1/state"'
        + ' or __name__="spock/actuator-1/value"}'
    ]
    assert [int(p['end'][0]) - int(p['start'][0]) for p in params][:3] == [6 * 3600] * 3
    assert all(params[i]['end'] == params[i + 1]['start'] for i in range(3))


async def test_csv_rejected(vic: victoria.VictoriaClient, url: str, httpx_mock: HTTPXMock):
    httpx_mock.add_response(url=f'{url}/api/v1/export', method='POST', status_code=422, text='bad query')
    args = TimeSeriesCsvQuery(fields=['a'], duration='1h', precision='ISO8601')
    with pytest.raises(httpx.HTTPStatusError):
        async for _ in vic.csv(args):
            pass


async def test_write(vic: victoria.VictoriaClient, now: datetime, written: list[str]):
    await vic.write(HistoryEvent(key='service', data={'f1': 1, 'f2': 'invalid'}))
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
        HistoryEvent(
            key='my "spark",1\\',
            data={
                'block, one': {'value=x': 1},
                'back\\slash\\': 2,
            },
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


async def test_write_dense(dense_vic: victoria.VictoriaClient, dense_url: str, httpx_mock: HTTPXMock):
    # Raw samples go to the dense database only
    httpx_mock.add_response(url=f'{dense_url}/write?precision=ms', method='POST')
    await dense_vic.write(HistoryEvent(key='service', data={'f1': 1}))
    assert [str(r.url) for r in httpx_mock.get_requests()] == [f'{dense_url}/write?precision=ms']


async def test_write_exc(
    vic: victoria.VictoriaClient, url: str, httpx_mock: HTTPXMock, caplog: pytest.LogCaptureFixture
):
    httpx_mock.add_exception(url=f'{url}/write?precision=ms', method='POST', exception=RuntimeError('dummy error'))

    # Write errors are logged with the database, and swallowed
    await vic.write(HistoryEvent(key='service', data={'f1': 1}))
    assert f'{url}/: write failed: RuntimeError(dummy error)' in caplog.text


async def test_write_rejected(
    vic: victoria.VictoriaClient, url: str, httpx_mock: HTTPXMock, caplog: pytest.LogCaptureFixture
):
    httpx_mock.add_response(url=f'{url}/write?precision=ms', method='POST', status_code=400, text='cannot parse line')

    # Rejected writes are logged with the database and its reason, and swallowed
    await vic.write(HistoryEvent(key='service', data={'f1': 1}))
    assert f'{url}/: write failed: HTTPStatusError' in caplog.text
    assert 'cannot parse line' in caplog.text


async def test_query_rejected(vic: victoria.VictoriaClient, url: str, httpx_mock: HTTPXMock):
    httpx_mock.add_response(url=f'{url}/api/v1/series', method='POST', status_code=422, text='bad query')

    with pytest.raises(httpx.HTTPStatusError):
        await vic.fields(TimeSeriesFieldsQuery())


async def test_close_dense(
    dense_vic: victoria.VictoriaClient, url: str, mocker: MockerFixture, caplog: pytest.LogCaptureFixture
):
    # A failing client does not keep the other open
    mocker.patch.object(dense_vic._archive, 'aclose', side_effect=RuntimeError('dummy'))
    await dense_vic.close()
    assert dense_vic._dense.is_closed
    assert f'{url}/: close failed: RuntimeError(dummy)' in caplog.text


async def test_lifespan(vic: victoria.VictoriaClient, mocker: MockerFixture):
    m_close = mocker.patch.object(vic, 'close', autospec=True)
    async with victoria.lifespan():
        m_close.assert_not_awaited()
    m_close.assert_awaited_once()
