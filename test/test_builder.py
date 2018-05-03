"""
Tests history.builder
"""

from unittest.mock import call

import pytest
from asynctest import CoroutineMock
from brewblox_history import builder

TESTED = builder.__name__


@pytest.fixture
def influx_mock(mocker):
    m = mocker.patch(TESTED + '.influx.get_client').return_value
    return m


@pytest.fixture
def query_mock(influx_mock):
    influx_mock.query = CoroutineMock(side_effect=lambda **kwargs: kwargs)
    return influx_mock.query


@pytest.fixture
async def app(app, mocker, query_mock):
    builder.setup(app)
    return app


@pytest.fixture
def field_keys_result():
    return {
        'results': [
            {
                'statement_id': 0,
                'series': [
                    {
                        'name': 'average_temperature',
                        'columns': [
                            'fieldKey',
                            'fieldType'
                        ],
                        'values': [
                            [
                                'degrees',
                                'float'
                            ]
                        ]
                    },
                    {
                        'name': 'h2o_feet',
                        'columns': [
                            'fieldKey',
                            'fieldType'
                        ],
                        'values': [
                            [
                                'level description',
                                'string'
                            ],
                            [
                                'water_level',
                                'float'
                            ]
                        ]
                    }
                ]
            }
        ]
    }


async def test_custom_query(app, client, query_mock):
    content = {
        'database': 'brewblox',
        'query': 'select * from controller'
    }

    assert (await client.post('/_debug/query', json=content)).status == 200
    query_mock.assert_called_once_with(**content)


async def test_list_objects(app, client, query_mock, field_keys_result):
    query_mock.side_effect = lambda **kwargs: field_keys_result

    resp = await client.post('/query/objects', json={})
    resp_content = await resp.json()

    assert resp_content == {
        'average_temperature': [
            'degrees'
        ],
        'h2o_feet': [
            'level description',
            'water_level'
        ]
    }

    await client.post('/query/objects', json={'measurement': 'measy', 'injection': 'drop tables'})
    await client.post('/query/objects', json={'database': 'the_internet'})

    assert query_mock.mock_calls == [
        call(database='brewblox', query='show field keys'),
        call(database='brewblox', query='show field keys from {measurement}', measurement='measy'),
        call(database='the_internet', query='show field keys')
    ]


async def test_get_values(app, client, query_mock):
    pass
    # measurement, objects, limit, start, end, granularity
    # measurement is required, others have defaults
