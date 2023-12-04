"""
Tests brewblox_history.victoria
"""

from datetime import datetime

import ciso8601
import pytest
from fastapi import FastAPI
from httpx import AsyncClient, Request, Response
from pytest_httpx import HTTPXMock
from pytest_mock import MockerFixture

from brewblox_history import victoria
from brewblox_history.models import (HistoryEvent, ServiceConfig,
                                     TimeSeriesCsvQuery, TimeSeriesFieldsQuery,
                                     TimeSeriesMetric, TimeSeriesMetricsQuery,
                                     TimeSeriesRange, TimeSeriesRangesQuery)

TESTED = victoria.__name__


@pytest.fixture
def url(config: ServiceConfig) -> str:
    return ''.join([
        config.victoria_protocol,
        '://',
        config.victoria_host,
        ':',
        str(config.victoria_port),
        config.victoria_path,
    ])


@pytest.fixture
def now(mocker: MockerFixture) -> datetime:
    dt = datetime(2021, 7, 15, 19)
    mocker.patch(TESTED + '.utils.now').side_effect = lambda: dt
    return dt


@pytest.fixture
def app():
    victoria.setup()
    app = FastAPI()
    return app


async def test_ping(client: AsyncClient, url: str, httpx_mock: HTTPXMock):
    vic = victoria.CV.get()

    httpx_mock.add_response(url=f'{url}/health',
                            method='GET',
                            text='OK')
    await vic.ping()

    httpx_mock.add_response(url=f'{url}/health',
                            method='GET',
                            text='NOK')
    with pytest.raises(ConnectionError):
        await vic.ping()


async def test_fields(client: AsyncClient, url: str, httpx_mock: HTTPXMock):
    vic = victoria.CV.get()

    httpx_mock.add_response(url=f'{url}/api/v1/series',
                            method='POST',
                            json={
                                'status': 'success',
                                'data': [
                                    {
                                        '__name__': 'spock/setpoint-sensor-pair-2/setting[degC]'
                                    },
                                    {
                                        '__name__': 'spock/actuator-1/value'
                                    },
                                    {
                                        '__name__': 'sparkey/HERMS MT PID/integralReset'
                                    },
                                    {
                                        '__name__': 'sparkey/HERMS HLT PID/inputValue[degC]'
                                    },
                                ]
                            })

    args = TimeSeriesFieldsQuery(duration='1d')
    assert await vic.fields(args) == [
        'sparkey/HERMS HLT PID/inputValue[degC]',
        'sparkey/HERMS MT PID/integralReset',
        'spock/actuator-1/value',
        'spock/setpoint-sensor-pair-2/setting[degC]',
    ]


async def test_metrics(client: AsyncClient,
                       url: str,
                       now: datetime,
                       httpx_mock: HTTPXMock):
    vic = victoria.CV.get()
    args = TimeSeriesMetricsQuery(fields=['service/f1', 'service/f2'])

    # No values cached yet
    assert await vic.metrics(args) == []

    # Don't return invalid values
    httpx_mock.add_response(url=f'{url}/write',
                            method='POST')
    await vic.write(HistoryEvent(key='service', data={'f1': 1, 'f2': 'invalid'}))
    result = await vic.metrics(args)
    assert result == [
        TimeSeriesMetric(metric='service/f1',
                         value=1,
                         timestamp=now)
    ]

    # Only update new values
    httpx_mock.add_response(url=f'{url}/write',
                            method='POST')
    await vic.write(HistoryEvent(key='service', data={'f2': 2}))
    result = await vic.metrics(args)
    assert result == [
        TimeSeriesMetric(metric='service/f1',
                         value=1,
                         timestamp=now),
        TimeSeriesMetric(metric='service/f2',
                         value=2,
                         timestamp=now),
    ]


async def test_ranges(client: AsyncClient, url: str, httpx_mock: HTTPXMock):
    vic = victoria.CV.get()

    result = {
        'metric': {'__name__': 'sparkey/sensor'},
        'values': [
            [1626367339.856, '1'],
            [1626367349.856, '2'],
            [1626367359.856, '3'],
        ],
    }

    httpx_mock.add_response(url=f'{url}/api/v1/query_range',
                            method='POST',
                            json={
                                'status': 'success',
                                'data': {
                                    'resultType': 'matrix',
                                    'result': [result],
                                },
                            })

    args = TimeSeriesRangesQuery(fields=['f1', 'f2', 'f3'])
    retv = await vic.ranges(args)
    assert retv == [TimeSeriesRange(**result)] * 3


async def test_csv(client: AsyncClient, url: str, httpx_mock: HTTPXMock):
    vic = victoria.CV.get()

    httpx_mock.add_response(
        url=f'{url}/api/v1/export',
        method='POST',
        text='\n'.join([
            '{"metric":{"__name__":"sparkey/HERMS BK PWM/setting"},' +
            '"values":[0,0,0,0,0,0,0],' +
            '"timestamps":[1626368070381,1626368075435,1626368080487,1626368085534,' +
            '1626368090630,1626368095687,1626368100749]}',

            '{"metric":{"__name__":"sparkey/HERMS BK PWM/setting"},' +
            '"values":[0,0,0,0],' +
            '"timestamps":[1626368105840,1626368110891,1626368115940,1626368121034]}',

            '{"metric":{"__name__":"spock/actuator-1/value"},' +
            '"values":[40,40,40,40,40,40,40,40,40,40,40,40,40],' +
            '"timestamps":[1626368060379,1626368060380,1626368070380,1626368078080,1626368083130,1626368088178,' +
            '1626368093272,1626368098328,1626368103383,1626368108480,1626368113533,1626368118579,1626368123669]}',

            '{"metric":{"__name__":"spock/pin-actuator-1/state"},' +
            '"values":[0,0,0,0,0,0,0,0,0,0,0],' +
            '"timestamps":[1626368070380,1626368078080,1626368083130,1626368088178,' +
            '1626368093272,1626368098328,1626368103383,1626368108480,1626368113533,1626368118579,1626368123669]}',
        ]))

    args = TimeSeriesCsvQuery(
        fields=[
            'sparkey/HERMS BK PWM/setting',
            'spock/pin-actuator-1/state',
            'spock/actuator-1/value'
        ],
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


async def test_write(client: AsyncClient,
                     url: str,
                     now: datetime,
                     httpx_mock: HTTPXMock):
    vic = victoria.CV.get()
    written = []

    async def handler(request: Request) -> Response:
        written.append(request.read().decode())
        return Response(200)

    httpx_mock.add_callback(url=f'{url}/write',
                            method='POST',
                            callback=handler)

    await vic.write(HistoryEvent(key='service', data={'f1': 1, 'f2': 'invalid'}))
    await vic.write(HistoryEvent(key='service', data={}))

    assert written == [
        'service f1=1.0'
    ]

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
