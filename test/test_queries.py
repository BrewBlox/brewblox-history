"""
Tests history.queries
"""

from unittest.mock import call

import pytest
from asynctest import CoroutineMock

from brewblox_history import queries

TESTED = queries.__name__


@pytest.fixture
def influx_mock(mocker):
    m = mocker.patch(TESTED + '.influx.get_client').return_value
    return m


@pytest.fixture
def query_mock(influx_mock):
    influx_mock.query = CoroutineMock(side_effect=lambda *args, **kwargs: {})
    return influx_mock.query


@pytest.fixture
async def app(app, mocker, query_mock):
    queries.setup(app)
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
                                1439856000000000000,
                                82,
                                'coyote_creek'
                            ],
                            [
                                1439856000000000000,
                                85,
                                'santa_monica'
                            ],
                            [
                                1439856360000000000,
                                73,
                                'coyote_creek'
                            ],
                            [
                                1439856360000000000,
                                74,
                                'santa_monica'
                            ],
                            [
                                1439856720000000000,
                                86,
                                'coyote_creek'
                            ],
                            [
                                1439856720000000000,
                                80,
                                'santa_monica'
                            ],
                            [
                                1439857080000000000,
                                89,
                                'coyote_creek'
                            ],
                            [
                                1439857080000000000,
                                81,
                                'santa_monica'
                            ],
                            [
                                1439857440000000000,
                                77,
                                'coyote_creek'
                            ],
                            [
                                1439857440000000000,
                                81,
                                'santa_monica'
                            ]
                        ]
                    }
                ]
            }
        ]
    }


@pytest.fixture
def count_result():
    return {
        'results': [
            {
                'statement_id': 0,
                'series': [
                    {
                        'name': 'brewblox_10s.autogen.pressure',
                        'columns': ['time', 'k1'],
                        'values': [[0, 600]],
                    }],
            },
            {
                'statement_id': 1,
                'series': [
                    {
                        'name': 'brewblox_1m.autogen.pressure',
                        'columns': ['time', 'k1'],
                        'values': [[0, 200]],
                    }],
            },
            {
                'statement_id': 2,
                'series': [
                    {
                        'name': 'brewblox_10m.autogen.pressure',
                        'columns': ['time', 'k1'],
                        'values': [[0, 100]],
                    }],
            },
            {
                'statement_id': 3,
                'series': [
                    {
                        'name': 'brewblox_1h.autogen.pressure',
                        'columns': ['time', 'k1'],
                        'values': [[0, 20]],
                    }],
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
        call(query='show field keys'),
        call(query='show field keys from "{measurement}"', measurement='measy'),
        call(database='the_internet', query='show field keys')
    ]


async def test_single_key(app, client, query_mock, values_result):
    """Asserts that ['single'] is split to 'single', and not 's,i,n,g,l,e'"""
    query_mock.side_effect = lambda **kwargs: values_result

    res = await client.post('/query/values', json={'measurement': 'm', 'fields': ['single']})
    assert res.status == 200

    query_mock.assert_called_once_with(
        query='select {fields} from "{measurement}"',
        measurement='m',
        fields='"single"'
    )


async def test_quote_fields(app, client, query_mock, values_result):
    """field keys must be quoted with double quotes. '*' is an exception."""
    query_mock.side_effect = lambda **kwargs: values_result

    res = await client.post('/query/values', json={'measurement': 'm', 'fields': ['first', 'second']})
    assert res.status == 200

    query_mock.assert_called_once_with(
        query='select {fields} from "{measurement}"',
        measurement='m',
        fields='"first","second"'
    )


async def test_value_data_format(app, client, query_mock, values_result):
    query_mock.side_effect = lambda **kwargs: values_result

    res = await client.post('/query/values', json={'measurement': 'm'})
    assert res.status == 200

    data = await res.json()
    assert data['name'] == 'average_temperature'
    assert len(data['values']) == 10
    assert data['columns'][1] == 'degrees'


@pytest.mark.parametrize('input_args, query_str', [
    (
        {},
        'select {fields} from "{measurement}"'
    ),
    (
        {'fields': ['you']},
        'select {fields} from "{measurement}"'
    ),
    (
        {'fields': ['key1', 'key2']},
        'select {fields} from "{measurement}"'
    ),
    (
        {'database': 'db'},
        'select {fields} from "{measurement}"'
    ),
    (
        {'start': '2018-10-10T12:00:00.000+02:00'},
        'select {fields} from "{measurement}" where time >= {start}'
    ),
    (
        {'start': '2018-10-10T12:00:00.000+02:00', 'duration': 'some time'},
        'select {fields} from "{measurement}" where time >= {start} and time <= {start} + {duration}'
    ),
    (
        {'start': '2018-10-10T12:00:00.000+02:00', 'end': '2018-10-10T12:00:00.000+02:00'},
        'select {fields} from "{measurement}" where time >= {start} and time <= {end}'
    ),
    (
        {'end': '2018-10-10T12:00:00.000+02:00'},
        'select {fields} from "{measurement}" where time <= {end}'
    ),
    (
        {'end': '2018-10-10T12:00:00.000+02:00', 'duration': 'bright side'},
        'select {fields} from "{measurement}" where time >= {end} - {duration} and time <= {end}'
    ),
    (
        {'duration': 'eternal'},
        'select {fields} from "{measurement}" where time >= now() - {duration}'
    ),
    (
        {'fields': ['key1', 'key2'], 'order_by': 'time desc', 'limit': 1},
        'select {fields} from "{measurement}" order by {order_by} limit {limit}'
    ),
    (
        {'duration': 'eternal', 'limit': 1},
        'select {fields} from "{measurement}" where time >= now() - {duration} limit {limit}'
    ),
    (
        {'database': 'db', 'fields': ['something', 'else'],
            'start': '2018-10-10T12:00:00.000+02:00', 'duration': '1d', 'limit': 5},
        'select {fields} from "{measurement}" where time >= {start} and time <= {start} + {duration} limit {limit}'
    )
])
async def test_get_values(input_args, query_str, app, client, influx_mock, query_mock, values_result):
    query_mock.side_effect = lambda **kwargs: values_result

    # Measurement is a required argument
    # Always add it to input_args
    input_args.setdefault('measurement', 'emmy')
    call_args = await queries.configure_params(influx_mock, **input_args)
    query_mock.reset_mock()

    # Mirrors transformation in API:
    # * Query string is created
    # * Keys are converted from a list to a comma separated string
    call_args['query'] = query_str
    quoted_keys = [f'"{k}"' for k in input_args.get('fields', [])] or ['*']
    call_args['fields'] = ','.join(quoted_keys)

    res = await client.post('/query/values', json=input_args)
    assert res.status == 200
    query_mock.assert_called_once_with(**call_args)


async def test_invalid_time_frame(app, client):
    res = await client.post('/query/values', json={
        'measurement': 'm',
        'start': '2018-10-10T12:00:00.000+02:00',
        'duration': '1m',
        'end': '2018-10-10T12:00:00.000+02:00'
    })
    assert res.status == 500
    assert 'ValueError' in await res.text()


async def test_unparsable_timeframe(app, client):
    res = await client.post('/query/values', json={'measurement': 'm', 'start': 'x'})
    assert res.status == 500
    assert 'ValueError' in await res.text()


async def test_no_values_found(app, client, query_mock):
    query_mock.side_effect = {'results': []}

    res = await client.post('/query/values', json={'measurement': 'm'})
    assert res.status == 200
    assert 'values' not in await res.json()


async def test_error_response(app, client, query_mock):
    query_mock.side_effect = RuntimeError('Whoops.')
    resp = await client.post('/query/objects', json={})

    assert resp.status == 500
    assert 'Whoops.' in await resp.text()


@pytest.mark.parametrize('approx_points, used_database', [
    # exact values
    (600, 'brewblox_10s'),
    (200, 'brewblox_1m'),
    (100, 'brewblox_10m'),
    (20, 'brewblox_1h'),
    # approximate
    (10000, 'brewblox_10s'),
    (500, 'brewblox_10s'),
    (40, 'brewblox_1h'),
])
async def test_select_downsampling_database(approx_points, used_database, app, client, query_mock, count_result):
    query_mock.side_effect = lambda **kwargs: count_result
    resp = await client.post('/query/values', json={
        'measurement': 'm',
        'fields': ['k1', 'k2'],
        'approx_points': approx_points
    })
    assert resp.status == 200
    print(query_mock.call_args_list)

    assert query_mock.call_args_list == [
        call(
            query=';'.join([
                f'select count(*) from "brewblox_{duration}".autogen."m" fill(0)'
                for duration in ['10s', '1m', '10m', '1h']
            ]),
            database='brewblox',
        ),
        call(
            query='select {fields} from "{measurement}"',
            fields='"mean_k1","mean_k2"',
            measurement='m',
            database=used_database,
            downsampled=True,
        )
    ]


async def test_empty_downsampling(app, client, query_mock):
    """
    Default to highest resolution (10s) when no rows are found in database
    """
    query_mock.side_effect = lambda **kwargs: {'results': [{'series': []}]}
    resp = await client.post('/query/values', json={
        'measurement': 'm',
        'fields': ['k1', 'k2'],
        'approx_points': 100
    })
    assert resp.status == 200

    assert query_mock.call_args_list == [
        call(
            query=';'.join([
                f'select count(*) from "brewblox_{duration}".autogen."m" fill(0)'
                for duration in ['10s', '1m', '10m', '1h']
            ]),
            database='brewblox',
        ),
        call(
            query='select {fields} from "{measurement}"',
            fields='"mean_k1","mean_k2"',
            measurement='m',
            database='brewblox_10s',
            downsampled=True,
        )
    ]
