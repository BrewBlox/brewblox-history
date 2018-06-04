"""
Tests brewblox_history.sse
"""

import json

import pytest
from asynctest import CoroutineMock
from brewblox_history import sse

TESTED = sse.__name__


@pytest.fixture
def values_result():
    return {
        'results': [
            {
                'statement_id': 0,
                'series': [
                    {
                        'name': 'average_temperature',
                        'columns': [
                            'time',
                            'degrees',
                            'location'
                        ],
                        'values': [
                            [
                                9000,
                                82,
                                'coyote_creek'
                            ],
                            [
                                9001,
                                85,
                                'santa_monica'
                            ]
                        ]
                    }
                ]
            }
        ]
    }


@pytest.fixture
def influx_mock(mocker):
    m = mocker.patch(TESTED + '.influx.get_client').return_value
    return m


@pytest.fixture
def interval_mock(mocker):
    mocker.patch(TESTED + '.POLL_INTERVAL_S', 0.001)


@pytest.fixture
async def app(app, influx_mock, interval_mock):
    sse.setup(app)
    return app


async def test_subscribe(app, client, influx_mock, values_result):
    influx_mock.query = CoroutineMock(side_effect=[{}, values_result, {}])
    res = await client.get('/sse/values', params={'measurement': 'm'})
    assert res.status == 200
    pushed = await res.text()
    assert json.loads(pushed[len('data:'):])  # SSE prefixes output with 'data: '
    assert influx_mock.query.call_count == 4
