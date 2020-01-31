"""
Tests brewblox_history.sse
"""

import asyncio
import json
from unittest.mock import AsyncMock
from urllib.parse import urlencode

import pytest

from brewblox_history import sse
from brewblox_service import features

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
    influx_mock.query = AsyncMock(side_effect=[
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
    influx_mock.query = AsyncMock(return_value=values_result)
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
    influx_mock.query = AsyncMock(side_effect=[{}])
    res = await client.get('/sse/values', params=urlencode(
        {'measurement': 'm', 'end': '2018-10-10T12:00:00.000+02:00', 'approx_points': 0},
        doseq=True
    ))
    assert res.status == 200
    pushed = await res.text()
    assert not pushed  # no data available
    assert influx_mock.query.call_count == 1


async def test_cancel_subscriptions(app, client, influx_mock, values_result):
    influx_mock.query = AsyncMock(return_value=values_result)
    signal = features.get(app, sse.ShutdownAlert).shutdown_signal

    async def close_after(delay):
        await asyncio.sleep(delay)
        signal.set()

    await asyncio.gather(
        client.get('/sse/values', params=urlencode({'measurement': 'm'}, doseq=True)),
        close_after(0.1)
    )


async def test_last_values_sse(app, client, influx_mock, last_values_result):
    influx_mock.query = AsyncMock(return_value=last_values_result)
    signal = features.get(app, sse.ShutdownAlert).shutdown_signal

    expected = [
        {
            'field': 'val1',
            'time': 1556527890131178000,
            'value': 0,
        },
        {
            'field': 'val2',
            'time': 1556527890131178000,
            'value': 100,
        },
        {
            'field': 'val_none',
            'time': None,
            'value': None,
        },
    ]
    expected_len = len(json.dumps(expected))
    prefix_len = len('data: ')

    async with client.get(
        '/sse/last_values',
        params=urlencode(
            {'measurement': 'sparkey', 'fields': ['val1', 'val2', 'val_none']},
            doseq=True)) as resp:
        resp_values = []

        while len(resp_values) < 3:
            read_val = (await resp.content.read(prefix_len + expected_len)).decode()
            # Skip new line characters
            if read_val.rstrip():
                resp_values.append(json.loads(read_val[prefix_len:]))

        assert resp_values == [expected, expected, expected]

        signal.set()
        await asyncio.sleep(0.1)

    assert resp.status == 200


async def test_last_values_sse_error(app, client, influx_mock):
    influx_mock.query = AsyncMock(side_effect=RuntimeError)

    resp = await client.get(
        '/sse/last_values',
        params=urlencode(
            {'measurement': 'sparkey', 'fields': ['val1', 'val2', 'val_none']},
            doseq=True))
    assert resp.status == 200  # stream responses return status before content
