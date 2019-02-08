"""
Tests brewblox_history.sse
"""

import asyncio
import json
from urllib.parse import urlencode

import pytest
from asynctest import CoroutineMock
from brewblox_service import features

from brewblox_history import sse

TESTED = sse.__name__


@pytest.fixture
def influx_mock(mocker):
    m = mocker.patch(TESTED + '.influx.get_client').return_value
    return m


@pytest.fixture
async def app(app, influx_mock):
    app['config']['poll_interval'] = 0.001
    sse.setup(app)
    return app


async def test_subscribe(app, client, influx_mock, policies_result, count_result, values_result):
    influx_mock.query = CoroutineMock(side_effect=[
        policies_result,
        count_result,
        values_result,
        values_result,
    ])
    res = await client.get('/sse/values', params=urlencode({
        'measurement': 'm',
        'fields': ['k1', 'k2'],
        'approx_points': 100
    },
        doseq=True
    ))
    assert res.status == 200
    # SSE prefixes output with 'data: '
    fragments = [json.loads(v) for v in (await res.text()).split('data:') if v]
    assert len(fragments) == 2
    # get policies, get point count, 3 * query, 1 * query error
    assert influx_mock.query.call_count == 5


async def test_subscribe_single(app, client, influx_mock, values_result):
    influx_mock.query = CoroutineMock(return_value=values_result)
    async with client.get('/sse/values', params=urlencode(
        {'measurement': 'm', 'end': '2018-10-10T12:00:00.000+02:00', 'approx_points': 0},
        doseq=True
    )) as resp:
        assert await resp.content.read(6) == b'data: '
        expected = values_result['results'][0]['series'][0]
        expected_json = json.dumps(expected)
        resp_values = await resp.content.read(len(expected_json))
        actual = json.loads(resp_values.decode())
        assert actual == expected


async def test_subscribe_single_no_data(app, client, influx_mock, values_result):
    influx_mock.query = CoroutineMock(side_effect=[{}])
    res = await client.get('/sse/values', params=urlencode(
        {'measurement': 'm', 'end': '2018-10-10T12:00:00.000+02:00', 'approx_points': 0},
        doseq=True
    ))
    assert res.status == 200
    pushed = await res.text()
    assert not pushed  # no data available
    assert influx_mock.query.call_count == 1


async def test_cancel_subscriptions(app, client, influx_mock, values_result):
    influx_mock.query = CoroutineMock(return_value=values_result)
    signal = features.get(app, sse.ShutdownAlert).shutdown_signal

    async def close_after(delay):
        await asyncio.sleep(delay)
        signal.set()

    await asyncio.gather(
        client.get('/sse/values', params=urlencode({'measurement': 'm'}, doseq=True)),
        close_after(0.1)
    )
