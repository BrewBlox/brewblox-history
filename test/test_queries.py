"""
Tests history.queries
"""

from unittest.mock import AsyncMock, call

import pytest

from brewblox_history import influx, queries, query_api
from brewblox_service.testing import response

TESTED = queries.__name__


@pytest.fixture
def influx_mock(mocker):
    m = mocker.patch(TESTED + '.influx.get_client').return_value
    return m


@pytest.fixture
def query_mock(influx_mock):
    influx_mock.query = AsyncMock(side_effect=lambda *args, **kwargs: {})
    return influx_mock.query


@pytest.fixture
async def app(app, mocker, query_mock):
    query_api.setup(app)
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


async def test_ping(app, client, influx_mock):
    influx_mock.ping = AsyncMock()
    await response(client.get('/ping'))


async def test_custom_query(app, client, query_mock):
    content = {
        'database': 'brewblox',
        'query': 'select * from controller'
    }

    await response(client.post('/_debug/query', json=content))
    query_mock.assert_called_once_with(**content)


async def test_list_objects(app, client, query_mock, field_keys_result):
    query_mock.side_effect = lambda **kwargs: field_keys_result

    resp_content = await response(client.post('/query/objects', json={}))

    assert resp_content == {
        'average_temperature': [
            'degrees'
        ],
        'h2o_feet': [
            'level description',
            'water_level'
        ]
    }

    await response(client.post('/query/objects', json={'measurement': 'measy', 'injection': 'drop tables'}))
    await response(client.post('/query/objects', json={'database': 'the_internet'}))

    assert query_mock.mock_calls == [
        call(query='SHOW FIELD KEYS'),
        call(query='SHOW FIELD KEYS FROM "{measurement}"', measurement='measy'),
        call(database='the_internet', query='SHOW FIELD KEYS')
    ]


async def test_single_key(app, client, query_mock, values_result):
    """Asserts that ['single'] is split to 'single', and not 's,i,n,g,l,e'"""
    query_mock.side_effect = lambda **kwargs: values_result

    await response(client.post('/query/values', json={'measurement': 'm', 'fields': ['single'], 'approx_points': 0}))

    query_mock.assert_called_once_with(
        query='SELECT {fields} FROM "{database}"."{policy}"."{measurement}"',
        fields='"single"',
        database=influx.DEFAULT_DATABASE,
        policy=influx.DEFAULT_POLICY,
        measurement='m',
        prefix='',
    )


async def test_quote_fields(app, client, query_mock, values_result):
    """field keys must be quoted with double quotes. '*' is an exception."""
    query_mock.side_effect = lambda **kwargs: values_result

    await response(client.post('/query/values', json={
        'measurement': 'm',
        'fields': ['first', 'second'],
        'approx_points': 0,
    }))

    query_mock.assert_called_once_with(
        query='SELECT {fields} FROM "{database}"."{policy}"."{measurement}"',
        fields='"first","second"',
        database=influx.DEFAULT_DATABASE,
        policy=influx.DEFAULT_POLICY,
        measurement='m',
        prefix='',
    )


async def test_value_data_format(app, client, query_mock, values_result):
    query_mock.side_effect = lambda **kwargs: values_result

    data = await response(client.post('/query/values', json={'measurement': 'm', 'approx_points': 0}))

    assert data['name'] == 'average_temperature'
    assert len(data['values']) == 10
    assert data['columns'][1] == 'degrees'


@pytest.mark.parametrize('input_args, query_str', [
    (
        {},
        'SELECT {fields} FROM "{database}"."{policy}"."{measurement}"'
    ),
    (
        {'fields': ['you']},
        'SELECT {fields} FROM "{database}"."{policy}"."{measurement}"'
    ),
    (
        {'fields': ['key1', 'key2']},
        'SELECT {fields} FROM "{database}"."{policy}"."{measurement}"'
    ),
    (
        {'database': 'db'},
        'SELECT {fields} FROM "{database}"."{policy}"."{measurement}"'
    ),
    (
        {'start': '2018-10-10T12:00:00.000+02:00'},
        'SELECT {fields} FROM "{database}"."{policy}"."{measurement}" ' +
        'WHERE time >= {start}'
    ),
    (
        {'start': '2018-10-10T12:00:00.000+02:00', 'duration': 'some time'},
        'SELECT {fields} FROM "{database}"."{policy}"."{measurement}" ' +
        'WHERE time >= {start} AND time <= {start} + {duration}'
    ),
    (
        {'start': '2018-10-10T12:00:00.000+02:00', 'end': '2018-10-10T12:00:00.000+02:00'},
        'SELECT {fields} FROM "{database}"."{policy}"."{measurement}" ' +
        'WHERE time >= {start} AND time <= {end}'
    ),
    (
        {'end': '2018-10-10T12:00:00.000+02:00'},
        'SELECT {fields} FROM "{database}"."{policy}"."{measurement}" ' +
        'WHERE time <= {end}'
    ),
    (
        {'end': '2018-10-10T12:00:00.000+02:00', 'duration': 'bright side'},
        'SELECT {fields} FROM "{database}"."{policy}"."{measurement}" ' +
        'WHERE time >= {end} - {duration} AND time <= {end}'
    ),
    (
        {'duration': 'eternal'},
        'SELECT {fields} FROM "{database}"."{policy}"."{measurement}" ' +
        'WHERE time >= now() - {duration}'
    ),
    (
        {'fields': ['key1', 'key2'], 'order_by': 'time desc', 'limit': 1},
        'SELECT {fields} FROM "{database}"."{policy}"."{measurement}" ' +
        'ORDER BY {order_by} LIMIT {limit}'
    ),
    (
        {'duration': 'eternal', 'limit': 1},
        'SELECT {fields} FROM "{database}"."{policy}"."{measurement}" ' +
        'WHERE time >= now() - {duration} LIMIT {limit}'
    ),
    (
        {'database': 'db', 'fields': ['something', 'else'],
            'start': '2018-10-10T12:00:00.000+02:00', 'duration': '1d', 'limit': 5},
        'SELECT {fields} FROM "{database}"."{policy}"."{measurement}" ' +
        'WHERE time >= {start} AND time <= {start} + {duration} LIMIT {limit}'
    )
])
async def test_get_values(input_args, query_str, app, client, influx_mock, query_mock, values_result):
    query_mock.side_effect = lambda **kwargs: values_result

    # Add default args
    input_args.setdefault('prefix', '')
    input_args.setdefault('policy', influx.DEFAULT_POLICY)
    input_args.setdefault('measurement', 'emmy')
    input_args.setdefault('approx_points', 0)

    call_args = await queries.configure_params(influx_mock, **input_args)
    query_mock.reset_mock()

    # Mirrors transformation in API:
    # * Query string is created
    # * Keys are converted from a list to a comma separated string
    call_args['query'] = query_str
    quoted_keys = [f'"{k}"' for k in input_args.get('fields', [])] or ['*']
    call_args['fields'] = ','.join(quoted_keys)

    await response(client.post('/query/values', json=input_args))
    query_mock.assert_called_once_with(**call_args)


async def test_invalid_time_frame(app, client):
    res = await response(client.post('/query/values', json={
        'measurement': 'm',
        'start': '2018-10-10T12:00:00.000+02:00',
        'duration': '1m',
        'end': '2018-10-10T12:00:00.000+02:00'
    }), 500)
    assert 'ValueError' in res['error']


async def test_unparsable_timeframe(app, client):
    res = await response(client.post('/query/values',
                                     json={'measurement': 'm', 'start': 'x'}),
                         500)
    assert 'Error' in res['error']


async def test_no_values_found(app, client, query_mock):
    query_mock.side_effect = {'results': []}

    res = await response(client.post('/query/values', json={'measurement': 'm', 'approx_points': 0}))
    assert 'values' not in res


async def test_error_response(app, client, query_mock):
    query_mock.side_effect = RuntimeError('Whoops.')
    resp = await response(client.post('/query/objects', json={}), 500)
    assert 'Whoops.' in resp['error']


@pytest.mark.parametrize('approx_points, used_policy, used_prefix', [
    # exact values
    (600, 'autogen', ''),
    (200, 'downsample_1m', 'm_'),
    (100, 'downsample_10m', 'm_m_'),
    (20, 'downsample_1h', 'm_m_m_'),
    (5, 'downsample_6h', 'm_m_m_m_'),
    # approximate
    (10000, 'autogen', ''),
    (500, 'autogen', ''),
    (40, 'downsample_10m', 'm_m_'),
    (1, 'downsample_6h', 'm_m_m_m_'),
])
async def test_select_policy(
        approx_points,
        used_policy,
        used_prefix,
        app,
        client,
        query_mock,
        policies_result,
        count_result,
        values_result):
    query_mock.side_effect = [policies_result, count_result, values_result]

    await response(client.post('/query/values', json={
        'measurement': 'm',
        'fields': ['k1', 'k2', used_prefix + 'v'],
        'approx_points': approx_points
    }))
    print(query_mock.call_args_list)

    assert query_mock.call_args_list == [
        call('SHOW RETENTION POLICIES ON "brewblox"'),
        call(';'.join([
            f'SELECT count(/(m_)*{influx.COMBINED_POINTS_FIELD}/) FROM "brewblox"."{policy}"."m"'
            for policy in [
                'autogen',
                'downsample_1m',
                'downsample_10m',
                'downsample_1h',
                'downsample_6h'
            ]
        ])),
        call(
            query='SELECT {fields} FROM "{database}"."{policy}"."{measurement}"',
            fields=f'"{used_prefix}k1","{used_prefix}k2","{used_prefix}{used_prefix}v"',
            measurement='m',
            database='brewblox',
            policy=used_policy,
            prefix=used_prefix,
        )
    ]


async def test_select_sparse_policy(app, client, query_mock, policies_result, count_result, values_result):
    """
    If the data interval is sufficiently sparse, the autogen policy can contain less data than downsampled policies.
    This will happen if
    - all policies have less than approx_points
    - requested period is > 24h (retention span of autogen)
    In this scenario, downsample_1m will have more points than autogen.
    """
    count_result['results'][0]['series'][0]['values'][0][1] = 180  # was 600
    query_mock.side_effect = [policies_result, count_result, values_result]

    await response(client.post('/query/values', json={
        'measurement': 'm',
        'fields': ['k1', 'k2'],
        'approx_points': 300
    }))
    print(query_mock.call_args_list)

    assert query_mock.call_args_list == [
        call('SHOW RETENTION POLICIES ON "brewblox"'),
        call(';'.join([
            f'SELECT count(/(m_)*{influx.COMBINED_POINTS_FIELD}/) FROM "brewblox"."{policy}"."m"'
            for policy in [
                'autogen',
                'downsample_1m',
                'downsample_10m',
                'downsample_1h',
                'downsample_6h'
            ]
        ])),
        call(
            query='SELECT {fields} FROM "{database}"."{policy}"."{measurement}"',
            fields=f'"m_k1","m_k2"',
            measurement='m',
            database='brewblox',
            policy='downsample_1m',
            prefix='m_',
        )
    ]


async def test_empty_downsampling(app, client, query_mock, policies_result, values_result):
    """
    Default to highest resolution (autogen) when no rows are found in database
    """
    query_mock.side_effect = [policies_result, {'results': [{'statement_id': id} for i in range(5)]}, values_result]
    await response(client.post('/query/values', json={
        'measurement': 'm',
        'fields': ['k1', 'k2'],
        'approx_points': 100
    }))

    assert query_mock.call_args_list == [
        call('SHOW RETENTION POLICIES ON "brewblox"'),
        call(';'.join([
            f'SELECT count(/(m_)*{influx.COMBINED_POINTS_FIELD}/) FROM "brewblox"."{policy}"."m"'
            for policy in [
                'autogen',
                'downsample_1m',
                'downsample_10m',
                'downsample_1h',
                'downsample_6h'
            ]
        ])),
        call(
            query='SELECT {fields} FROM "{database}"."{policy}"."{measurement}"',
            fields=f'"k1","k2"',
            measurement='m',
            database=influx.DEFAULT_DATABASE,
            policy=influx.DEFAULT_POLICY,
            prefix='',
        )
    ]


async def test_configure(app, client, query_mock):
    query_mock.side_effect = lambda *args, **kwargs: {'configure': True}
    resp = await response(client.post('/query/configure'))
    assert resp == {'configure': True}
    # 5 * create / alter policy
    # 5 * drop / create continuous query
    # 1 * status query
    assert query_mock.call_count == (5*2) + (4*2) + 1


async def test_select_last_values(app, client, query_mock, last_values_result):
    query_mock.side_effect = lambda **kwargs: last_values_result

    resp = await response(client.post('/query/last_values', json={
        'measurement': 'sparkey',
        'fields': ['val1', 'val2', 'val_none'],

    }))
    assert resp == [
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
