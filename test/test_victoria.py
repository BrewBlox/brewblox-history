"""
Tests brewblox_history.victoria
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone

import ciso8601
import httpx
import pytest
from httpx import Request, Response
from pytest_httpx import HTTPXMock
from pytest_mock import MockerFixture

from brewblox_history import utils, victoria
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
def vic() -> victoria.VictoriaClient:
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
    result = {
        'metric': {'__name__': 'sparkey/sensor'},
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
                'result': [result],
            },
        },
        is_reusable=True,
    )

    args = TimeSeriesRangesQuery(fields=['f1', 'f2', 'f3'])
    retv = await vic.ranges(args)
    assert retv == [TimeSeriesRange(**result)] * 3


async def test_csv(vic: victoria.VictoriaClient, url: str, httpx_mock: HTTPXMock):
    httpx_mock.add_response(
        url=f'{url}/api/v1/export',
        method='POST',
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


async def test_write_database(vic: victoria.VictoriaClient):
    # Against the real database: names as stored, and timestamps
    key = 'itest "spark",1\\'
    data = {'block, one': {'value=x': 1}, 'back\\slash\\': 2, 'temp[°C]': 3}
    timestamp = utils.to_millis(utils.now()) - 2000

    await vic.write(HistoryEvent(key=key, data=data, timestamp=timestamp))
    await vic.write(HistoryEvent(key=key, data={'arrival': 4}))

    stamped = {f'{key}/block, one/value=x', f'{key}/back\\slash\\', f'{key}/temp[°C]'}
    expected = stamped | {f'{key}/arrival'}
    rows = {}
    for _ in range(50):  # New samples become searchable about a second after a forced flush
        (await vic._client.get('/internal/force_flush')).raise_for_status()
        resp = await vic._client.post('/api/v1/export', data={'match[]': '{__name__=~"itest.*"}'})
        resp.raise_for_status()
        rows = {row['metric']['__name__']: row for row in map(json.loads, resp.text.splitlines())}
        if expected <= rows.keys():
            break
        await asyncio.sleep(0.1)

    assert rows.keys() == expected
    for name in stamped:
        assert rows[name]['timestamps'] == [timestamp]
    assert rows[f'{key}/arrival']['timestamps'][0] > timestamp


async def test_write_exc(vic: victoria.VictoriaClient, url: str, httpx_mock: HTTPXMock):
    httpx_mock.add_exception(url=f'{url}/write?precision=ms', method='POST', exception=RuntimeError('dummy error'))

    # Write errors are swallowed
    await vic.write(HistoryEvent(key='service', data={'f1': 1}))


async def test_write_rejected(
    vic: victoria.VictoriaClient, url: str, httpx_mock: HTTPXMock, caplog: pytest.LogCaptureFixture
):
    httpx_mock.add_response(url=f'{url}/write?precision=ms', method='POST', status_code=400, text='cannot parse line')

    # Rejected writes are logged with the database's reason, and swallowed
    await vic.write(HistoryEvent(key='service', data={'f1': 1}))
    assert 'cannot parse line' in caplog.text


async def test_query_rejected(vic: victoria.VictoriaClient, url: str, httpx_mock: HTTPXMock):
    httpx_mock.add_response(url=f'{url}/api/v1/series', method='POST', status_code=422, text='bad query')

    with pytest.raises(httpx.HTTPStatusError):
        await vic.fields(TimeSeriesFieldsQuery())


async def test_lifespan(vic: victoria.VictoriaClient, mocker: MockerFixture):
    m_close = mocker.patch.object(vic, 'close', autospec=True)
    async with victoria.lifespan():
        m_close.assert_not_awaited()
    m_close.assert_awaited_once()
