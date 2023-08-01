"""
Tests brewblox_history.victoria
"""

from datetime import datetime
from unittest.mock import ANY

import ciso8601
import pytest
from aiohttp import web
from aresponses import ResponsesMockServer
from brewblox_service import http, scheduler

from brewblox_history import victoria
from brewblox_history.models import (HistoryEvent, TimeSeriesCsvQuery,
                                     TimeSeriesFieldsQuery, TimeSeriesMetric,
                                     TimeSeriesMetricsQuery, TimeSeriesRange,
                                     TimeSeriesRangesQuery)

TESTED = victoria.__name__


@pytest.fixture
async def setup(app):
    scheduler.setup(app)
    http.setup(app)
    victoria.setup(app)


async def test_ping(app, client, aresponses: ResponsesMockServer):
    vic = victoria.fget(app)

    aresponses.add(
        path_pattern='/victoria/health',
        method_pattern='GET',
        response='OK'
    )
    await vic.ping()

    aresponses.add(
        path_pattern='/victoria/health',
        method_pattern='GET',
        response='NOK'
    )
    with pytest.raises(ConnectionError):
        await vic.ping()


async def test_fields(app, client, aresponses: ResponsesMockServer):
    vic = victoria.fget(app)

    aresponses.add(
        path_pattern='/victoria/api/v1/series',
        method_pattern='POST',
        response={
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
        }
    )

    args = TimeSeriesFieldsQuery(duration='1d')
    assert await vic.fields(args) == [
        'sparkey/HERMS HLT PID/inputValue[degC]',
        'sparkey/HERMS MT PID/integralReset',
        'spock/actuator-1/value',
        'spock/setpoint-sensor-pair-2/setting[degC]',
    ]


async def test_metrics(app, client):
    vic = victoria.fget(app)
    args = TimeSeriesMetricsQuery(fields=['service/f1', 'service/f2'])

    # No values cached yet
    assert await vic.metrics(args) == []

    # Don't return invalid values
    await vic.write(HistoryEvent(key='service', data={'f1': 1, 'f2': 'invalid'}))
    assert await vic.metrics(args) == [
        {
            'metric': 'service/f1',
            'value': 1,
            'timestamp': ANY,
        },
    ]

    # Only update new values
    await vic.write(HistoryEvent(key='service', data={'f2': 2}))
    assert await vic.metrics(args) == [
        {
            'metric': 'service/f1',
            'value': 1,
            'timestamp': ANY,
        },
        {
            'metric': 'service/f2',
            'value': 2,
            'timestamp': ANY,
        }
    ]


async def test_ranges(app, client, aresponses: ResponsesMockServer):
    vic = victoria.fget(app)

    result = {
        'metric': {'__name__': 'sparkey/sensor'},
        'values': [
            [1626367339.856, '1'],
            [1626367349.856, '2'],
            [1626367359.856, '3'],
        ],
    }

    aresponses.add(
        path_pattern='/victoria/api/v1/query_range',
        method_pattern='POST',
        repeat=3,
        response={
            'status': 'success',
            'data': {
                'resultType': 'matrix',
                'result': [result],
            },
        },
    )

    args = TimeSeriesRangesQuery(fields=['f1', 'f2', 'f3'])
    retv = await vic.ranges(args)
    assert retv == [TimeSeriesRange(**result)] * 3


async def test_csv(app, client, aresponses: ResponsesMockServer):
    vic = victoria.fget(app)

    aresponses.add(
        path_pattern='/victoria/api/v1/export',
        method_pattern='POST',
        response='\n'.join([
            '{"metric":{"__name__":"sparkey/HERMS BK PWM/setting"},' +
            '"values":[0,0,0,0,0,0,0],' +
            '"timestamps":[1626368070381,1626368075435,1626368080487,1626368085534,' +
            '1626368090630,1626368095687,1626368100749]}',

            '{"metric":{"__name__":"sparkey/HERMS BK PWM/setting"},' +
            '"values":[0,0,0,0],' +
            '"timestamps":[1626368105840,1626368110891,1626368115940,1626368121034]}',

            '{"metric":{"__name__":"spock/actuator-1/value"},' +
            '"values":[40,40,40,40,40,40,40,40,40,40,40],' +
            '"timestamps":[1626368070380,1626368078080,1626368083130,1626368088178,' +
            '1626368093272,1626368098328,1626368103383,1626368108480,1626368113533,1626368118579,1626368123669]}',

            '{"metric":{"__name__":"spock/pin-actuator-1/state"},' +
            '"values":[0,0,0,0,0,0,0,0,0,0,0],' +
            '"timestamps":[1626368070380,1626368078080,1626368083130,1626368088178,' +
            '1626368093272,1626368098328,1626368103383,1626368108480,1626368113533,1626368118579,1626368123669]}',
        ])
    )
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
    assert len(result) == 23  # headers, 11 from sparkey, 11 from spock
    assert result[0] == ','.join(['time'] + args.fields)

    # line 1: values from spock
    line = result[1].split(',')
    assert ciso8601.parse_datetime(line[0])
    assert line[1:] == ['', '0', '40']

    # line 2: values from sparkey
    line = result[2].split(',')
    assert ciso8601.parse_datetime(line[0])
    assert line[1:] == ['0', '', '']


async def test_write(app, mocker, client, aresponses: ResponsesMockServer):
    def now() -> datetime:
        return datetime(2021, 7, 15, 19)

    mocker.patch(TESTED + '.utils.now').side_effect = now

    vic = victoria.fget(app)
    written = []

    async def handler(request):
        written.append(await request.text())
        return web.Response()

    aresponses.add(
        path_pattern='/victoria/write',
        method_pattern='POST',
        repeat=aresponses.INFINITY,
        response=handler,
    )

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
            timestamp=now(),
        ),
    ]

    await vic.write(HistoryEvent(key='service', data={'f1': 2, 'f2': 3}))
    assert written == [
        'service f1=1.0',
        'service f1=2.0,f2=3.0',
    ]
