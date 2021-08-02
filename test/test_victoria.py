"""
Tests brewblox_history.victoria
"""

import asyncio

import ciso8601
import pytest
from aiohttp import web
from aresponses import ResponsesMockServer
from brewblox_service import http, scheduler
from mock import ANY

from brewblox_history import victoria

TESTED = victoria.__name__


@pytest.fixture
async def app(app):
    scheduler.setup(app)
    http.setup(app)
    victoria.setup(app)
    return app


@pytest.fixture
async def m_write_interval(app):
    victoria.fget(app)._write_interval = 0.001


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

    assert await vic.fields('1d') == [
        'sparkey/HERMS HLT PID/inputValue[degC]',
        'sparkey/HERMS MT PID/integralReset',
        'spock/actuator-1/value',
        'spock/setpoint-sensor-pair-2/setting[degC]',
    ]


async def test_metrics(app, client):
    vic = victoria.fget(app)
    await vic.end()  # We don't want to actually write values

    # No values cached yet
    assert await vic.metrics(['service/f1', 'service/f2']) == []

    # Don't return invalid values
    vic.write_soon('service', {'f1': 1, 'f2': 'invalid'})
    assert await vic.metrics(['service/f1', 'service/f2']) == [
        {
            'metric': 'service/f1',
            'value': 1,
            'timestamp': ANY,
        },
    ]

    # Only update new values
    vic.write_soon('service', {'f2': 2})
    assert await vic.metrics(['service/f1', 'service/f2']) == [
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

    assert await vic.ranges(['f1', 'f2', 'f3']) == [result] * 3


async def test_csv(app, client, aresponses: ResponsesMockServer):
    vic = victoria.fget(app)

    aresponses.add(
        path_pattern='/victoria/api/v1/export',
        method_pattern='POST',
        response='\n'.join([
            '{"metric":{"__name__":"sparkey/HERMS BK PWM/setting"},' +
            '"values":[0,0,0,0,0,0,0,0,0,0,0],' +
            '"timestamps":[1626368070381,1626368075435,1626368080487,1626368085534,' +
            '1626368090630,1626368095687,1626368100749,1626368105840,1626368110891,1626368115940,1626368121034]}',

            '{"metric":{"__name__":"spock/actuator-1/value"},' +
            '"values":[40,40,40,40,40,40,40,40,40,40,40],' +
            '"timestamps":[1626368072988,1626368078080,1626368083130,1626368088178,' +
            '1626368093272,1626368098328,1626368103383,1626368108480,1626368113533,1626368118579,1626368123669]}',

            '{"metric":{"__name__":"spock/pin-actuator-1/state"},' +
            '"values":[0,0,0,0,0,0,0,0,0,0,0],' +
            '"timestamps":[1626368072988,1626368078080,1626368083130,1626368088178,' +
            '1626368093272,1626368098328,1626368103383,1626368108480,1626368113533,1626368118579,1626368123669]}',
        ])
    )

    fields = [
        'sparkey/HERMS BK PWM/setting',
        'spock/pin-actuator-1/state',
        'spock/actuator-1/value'
    ]
    result = []
    async for line in vic.csv(fields, precision='ISO8601'):
        result.append(line)
    assert len(result) == 23  # headers, 11 from sparkey, 11 from spock
    assert result[0] == ','.join(['time'] + fields)

    # line 1: values from sparkey
    line = result[1].split(',')
    assert ciso8601.parse_datetime(line[0])
    assert line[1:] == ['0', '', '']

    # line 2: values from spock
    line = result[2].split(',')
    assert ciso8601.parse_datetime(line[0])
    assert line[1:] == ['', '0', '40']


async def test_write_soon(app, mocker, m_write_interval, client, aresponses: ResponsesMockServer):
    ns_time = int(1e9)
    mocker.patch(TESTED + '.time.time_ns').return_value = ns_time
    vic = victoria.fget(app)

    written = []

    async def handler(request):
        written.append(await request.text())
        return web.Response()

    aresponses.add(
        path_pattern='/victoria/write',
        method_pattern='GET',
        repeat=aresponses.INFINITY,
        response=handler,
    )

    vic.write_soon('service', {'f1': 1, 'f2': 'invalid'})
    vic.write_soon('service', {})

    await asyncio.sleep(0.1)
    assert written == [
        f'service f1=1.0 {ns_time}'
    ]

    assert await vic.metrics(['service/f1']) == [{
        'metric': 'service/f1',
        'value': 1.0,
        'timestamp': 1000,
    }]

    vic.write_soon('service', {'f1': 2, 'f2': 3})
    await asyncio.sleep(0.1)
    assert written == [
        f'service f1=1.0 {ns_time}',
        f'service f1=2.0,f2=3.0 {ns_time}',
    ]


async def test_write_error(app, mocker, m_write_interval, client, aresponses: ResponsesMockServer):
    vic = victoria.fget(app)

    aresponses.add(
        path_pattern='/victoria/write',
        method_pattern='GET',
        repeat=aresponses.INFINITY,
        response=web.Response(status=400),
    )

    vic.write_soon('service', {'f1': 1, 'f2': 'invalid'})
    vic.write_soon('service', {'f1': 2, 'f2': 'invalid'})
    vic.write_soon('service', {'f1': 3, 'f2': 'invalid'})
    vic.write_soon('service', {})

    await asyncio.sleep(0.1)

    assert await vic.metrics(['service/f1']) == [{
        'metric': 'service/f1',
        'value': 3.0,
        'timestamp': ANY,
    }]

    assert len(vic._pending_lines) == 3

    mocker.patch(TESTED + '.MAX_PENDING_LINES', 3)
    await asyncio.sleep(0.1)
    assert len(vic._pending_lines) == 2
