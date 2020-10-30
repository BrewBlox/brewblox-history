"""
Tests history.history_api
"""

import asyncio

import pytest
from aiohttp import ClientWebSocketResponse, WSMsgType
from brewblox_service import features
from brewblox_service.testing import response
from mock import AsyncMock, call

from brewblox_history import history_api, influx, queries

TESTED = history_api.__name__


@pytest.fixture
def influx_mock(mocker):
    m = mocker.patch(TESTED + '.influx.fget_client').return_value
    return m


@pytest.fixture
def query_mock(influx_mock):
    influx_mock.query = AsyncMock(side_effect=lambda *args, **kwargs: {})
    return influx_mock.query


@pytest.fixture
async def app(app, mocker, query_mock):
    app['config']['poll_interval'] = 0.05
    history_api.setup(app)
    return app


async def test_ping(app, client, influx_mock):
    influx_mock.ping = AsyncMock()
    await response(client.get('/history/ping'))


async def test_custom_query(app, client, query_mock):
    content = {
        'query': 'select * from controller'
    }

    await response(client.post('/_debug/query', json=content))
    query_mock.assert_called_once_with(**content)


async def test_show_fields(app, client, query_mock, field_keys_result):
    query_mock.side_effect = lambda **kwargs: field_keys_result

    resp_content = await response(client.post('/history/fields', json={}))

    assert resp_content == {
        'average_temperature': [
            'degrees'
        ],
        'h2o_feet': [
            'level description',
            'water_level'
        ]
    }

    await response(client.post('/history/fields', json={'measurement': 'measy', 'injection': 'drop tables'}))

    assert query_mock.mock_calls == [
        call(query='SHOW FIELD KEYS'),
        call(query='SHOW FIELD KEYS FROM "{measurement}"', measurement='measy'),
    ]


async def test_show_all_fields(app, client, query_mock, measurements_result, all_field_keys_result):
    query_mock.side_effect = [measurements_result, all_field_keys_result, all_field_keys_result]
    resp_content = await response(client.post('/history/fields', json={'include_stale': True}))

    assert resp_content == {
        'iSpindel000': [' Combined Influx points', 'angle', 'battery'],
        'ispindel': [' Combined Influx points', 'angle', 'battery'],
        'plaato': [' Combined Influx points', 'abv', 'bpm'],
        'spark-one': [
            ' Combined Influx points',
            'ActiveGroups/active/0',
            'ActiveGroups/active/1',
            'Case Temp Sensor/value[degC]'
        ]
    }

    resp_content_single = await response(client.post('/history/fields', json={
        'include_stale': True,
        'measurement': 'sparkey'
    }))
    assert resp_content_single == resp_content

    assert query_mock.call_count == 3


async def test_single_key(app, client, query_mock, values_result):
    """Asserts that ['single'] is split to 'single', and not 's,i,n,g,l,e'"""
    query_mock.side_effect = lambda **kwargs: values_result

    await response(client.post('/history/values', json={'measurement': 'm', 'fields': ['single'], 'approx_points': 0}))

    query_mock.assert_called_once_with(
        query='SELECT {fields} FROM "{policy}"."{measurement}"',
        fields='"single"',
        policy=influx.DEFAULT_POLICY,
        measurement='m',
        prefix='',
    )


async def test_quote_fields(app, client, query_mock, values_result):
    """field keys must be quoted with double quotes. '*' is an exception."""
    query_mock.side_effect = lambda **kwargs: values_result

    await response(client.post('/history/values', json={
        'measurement': 'm',
        'fields': ['first', 'second'],
        'approx_points': 0,
    }))

    query_mock.assert_called_once_with(
        query='SELECT {fields} FROM "{policy}"."{measurement}"',
        fields='"first","second"',
        policy=influx.DEFAULT_POLICY,
        measurement='m',
        prefix='',
    )


async def test_value_data_format(app, client, query_mock, values_result):
    query_mock.side_effect = lambda **kwargs: values_result

    data = await response(client.post('/history/values', json={'measurement': 'm', 'approx_points': 0}))

    assert data['name'] == 'average_temperature'
    assert len(data['values']) == 10
    assert data['columns'][1] == 'degrees'


@pytest.mark.parametrize('input_args, query_str', [
    (
        {},
        'SELECT {fields} FROM "{policy}"."{measurement}"'
    ),
    (
        {'fields': ['you']},
        'SELECT {fields} FROM "{policy}"."{measurement}"'
    ),
    (
        {'fields': ['key1', 'key2']},
        'SELECT {fields} FROM "{policy}"."{measurement}"'
    ),
    (
        {'database': 'db'},
        'SELECT {fields} FROM "{policy}"."{measurement}"'
    ),
    (
        {'start': '2018-10-10T12:00:00.000+02:00'},
        'SELECT {fields} FROM "{policy}"."{measurement}" ' +
        'WHERE time >= {start}'
    ),
    (
        {'start': '2018-10-10T12:00:00.000+02:00', 'duration': 'some time'},
        'SELECT {fields} FROM "{policy}"."{measurement}" ' +
        'WHERE time >= {start} AND time <= {start} + {duration}'
    ),
    (
        {'start': '2018-10-10T12:00:00.000+02:00', 'end': '2018-10-10T12:00:00.000+02:00'},
        'SELECT {fields} FROM "{policy}"."{measurement}" ' +
        'WHERE time >= {start} AND time <= {end}'
    ),
    (
        {'end': '2018-10-10T12:00:00.000+02:00'},
        'SELECT {fields} FROM "{policy}"."{measurement}" ' +
        'WHERE time <= {end}'
    ),
    (
        {'end': '2018-10-10T12:00:00.000+02:00', 'duration': 'bright side'},
        'SELECT {fields} FROM "{policy}"."{measurement}" ' +
        'WHERE time >= {end} - {duration} AND time <= {end}'
    ),
    (
        {'duration': 'eternal'},
        'SELECT {fields} FROM "{policy}"."{measurement}" ' +
        'WHERE time >= now() - {duration}'
    ),
    (
        {'fields': ['key1', 'key2'], 'order_by': 'time desc', 'limit': 1},
        'SELECT {fields} FROM "{policy}"."{measurement}" ' +
        'ORDER BY {order_by} LIMIT {limit}'
    ),
    (
        {'duration': 'eternal', 'limit': 1, 'epoch': 'ms'},
        'SELECT {fields} FROM "{policy}"."{measurement}" ' +
        'WHERE time >= now() - {duration} LIMIT {limit}'
    ),
    (
        {'database': 'db', 'fields': ['something', 'else'],
            'start': '2018-10-10T12:00:00.000+02:00', 'duration': '1d', 'limit': 5},
        'SELECT {fields} FROM "{policy}"."{measurement}" ' +
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

    await response(client.post('/history/values', json=input_args))
    query_mock.assert_called_once_with(**call_args)


async def test_invalid_time_frame(app, client):
    res = await response(client.post('/history/values', json={
        'measurement': 'm',
        'start': '2018-10-10T12:00:00.000+02:00',
        'duration': '1m',
        'end': '2018-10-10T12:00:00.000+02:00'
    }), 500)
    assert 'ValueError' in res['error']


async def test_unparsable_timeframe(app, client):
    res = await response(client.post('/history/values',
                                     json={'measurement': 'm', 'start': 'x'}),
                         500)
    assert 'Error' in res['error']


async def test_no_values_found(app, client, query_mock):
    query_mock.side_effect = {'results': []}

    res = await response(client.post('/history/values', json={'measurement': 'm', 'approx_points': 0}))
    assert 'values' not in res


async def test_error_response(app, client, query_mock):
    query_mock.side_effect = RuntimeError('Whoops.')
    resp = await response(client.post('/history/fields', json={}), 500)
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
        count_result,
        values_result):
    query_mock.side_effect = [count_result, values_result]

    await response(client.post('/history/values', json={
        'measurement': 'm',
        'fields': ['k1', 'k2', used_prefix + 'v'],
        'approx_points': approx_points
    }))
    print(query_mock.call_args_list)

    assert query_mock.call_args_list == [
        call(';'.join([
            f'SELECT count(/(m_)*{influx.COMBINED_POINTS_FIELD}/) FROM "{policy}"."m"'
            for policy in [
                'autogen',
                'downsample_1m',
                'downsample_10m',
                'downsample_1h',
                'downsample_6h'
            ]
        ])),
        call(
            query='SELECT {fields} FROM "{policy}"."{measurement}"',
            policy=used_policy,
            measurement='m',
            fields=f'"{used_prefix}k1","{used_prefix}k2","{used_prefix}{used_prefix}v"',
            prefix=used_prefix,
        )
    ]


async def test_select_sparse_policy(app, client, query_mock, count_result, values_result):
    """
    If the data interval is sufficiently sparse, the autogen policy can contain less data than downsampled policies.
    This will happen if
    - all policies have less than approx_points
    - requested period is > 24h (retention span of autogen)
    In this scenario, downsample_1m will have more points than autogen.
    """
    count_result['results'][0]['series'][0]['values'][0][1] = 180  # was 600
    query_mock.side_effect = [count_result, values_result]

    await response(client.post('/history/values', json={
        'measurement': 'm',
        'fields': ['k1', 'k2'],
        'approx_points': 300
    }))
    print(query_mock.call_args_list)

    assert query_mock.call_args_list == [
        call(';'.join([
            f'SELECT count(/(m_)*{influx.COMBINED_POINTS_FIELD}/) FROM "{policy}"."m"'
            for policy in [
                'autogen',
                'downsample_1m',
                'downsample_10m',
                'downsample_1h',
                'downsample_6h'
            ]
        ])),
        call(
            query='SELECT {fields} FROM "{policy}"."{measurement}"',
            policy='downsample_1m',
            measurement='m',
            fields='"m_k1","m_k2"',
            prefix='m_',
        )
    ]


async def test_empty_downsampling(app, client, query_mock, values_result):
    """
    Default to highest resolution (autogen) when no rows are found in database
    """
    query_mock.side_effect = [{'results': [{'statement_id': id} for i in range(5)]}, values_result]
    await response(client.post('/history/values', json={
        'measurement': 'm',
        'fields': ['k1', 'k2'],
        'approx_points': 100
    }))

    assert query_mock.call_args_list == [
        call(';'.join([
            f'SELECT count(/(m_)*{influx.COMBINED_POINTS_FIELD}/) FROM "{policy}"."m"'
            for policy in [
                'autogen',
                'downsample_1m',
                'downsample_10m',
                'downsample_1h',
                'downsample_6h'
            ]
        ])),
        call(
            query='SELECT {fields} FROM "{policy}"."{measurement}"',
            policy=influx.DEFAULT_POLICY,
            measurement='m',
            fields='"k1","k2"',
            prefix='',
        )
    ]


async def test_exclude_autogen(app, client, query_mock, count_result, values_result):
    """
    If start is more than 24h ago, autogen should be excluded.
    """
    del count_result['results'][0]
    query_mock.side_effect = [count_result, values_result]

    await response(client.post('/history/values', json={
        'measurement': 'm',
        'fields': ['k1', 'k2'],
        'approx_points': 300,
        'duration': '30h',
    }))
    print(query_mock.call_args_list)

    assert query_mock.call_args_list == [
        call(
            ';'.join([
                (f'SELECT count(/(m_)*{influx.COMBINED_POINTS_FIELD}/) FROM "{policy}"."m"'
                 + ' WHERE time >= now() - {duration}')
                for policy in [
                    'downsample_1m',
                    'downsample_10m',
                    'downsample_1h',
                    'downsample_6h'
                ]
            ]),
            duration='30h'),
        call(
            query='SELECT {fields} FROM "{policy}"."{measurement}" WHERE time >= now() - {duration}',
            policy='downsample_1m',
            measurement='m',
            fields='"m_k1","m_k2"',
            duration='30h',
            prefix='m_',
        )
    ]


async def test_configure(app, client, query_mock):
    query_mock.side_effect = lambda *args, **kwargs: {'configure': True}
    await response(client.post('/history/configure'))
    # 5 * create / alter policy
    # 5 * drop / create continuous query
    assert query_mock.call_count == (5 * 2) + (4 * 2)


async def test_invalid_data(app, client, query_mock):
    await response(client.post('/history/last_values'), 422)


async def test_select_last_values(app, client, query_mock, last_values_result):
    query_mock.side_effect = lambda **kwargs: last_values_result

    resp = await response(client.post('/history/last_values', json={
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

    await response(client.post('/history/last_values', json={
        'measurement': 'sparkey',
        'fields': ['val1', 'val2', 'val_none'],
        'policy': 'economic'
    }), 500)


async def test_stream_noop(app, client, query_mock):
    async with client.ws_connect('/history/stream') as ws:
        with pytest.raises(asyncio.TimeoutError):
            await ws.receive_json(timeout=0.1)


async def test_stream_values_once(app, client, query_mock, count_result, values_result):
    query_mock.side_effect = [count_result, values_result]

    async with client.ws_connect('/history/stream') as ws:
        ws: ClientWebSocketResponse
        await ws.send_json({
            'id': 'test',
            'command': 'values',
            'query': {
                'measurement': 'm',
                'fields': ['k1', 'k2'],
                'end': '2018-10-10T12:00:00.000+02:00',
            },
        })
        resp = await ws.receive_json(timeout=2)
        print(resp)
        assert resp['data']['columns']


async def test_stream_values(app, client, query_mock, count_result, values_result):
    query_mock.side_effect = [count_result] + [{'results': []}] + [values_result]*100

    async with client.ws_connect('/history/stream') as ws:
        ws: ClientWebSocketResponse
        await ws.send_json({
            'id': 'test',
            'command': 'values',
            'query': {
                'measurement': 'm',
                'fields': ['k1', 'k2'],
            },
        })
        resp1 = await ws.receive_json(timeout=2)
        resp2 = await ws.receive_json(timeout=2)
        print(resp1)
        assert resp1['data']['initial'] is True
        assert resp2['data']['initial'] is False
        # Otherwise same
        resp1['data']['initial'] = False
        assert resp1 == resp2


async def test_stream_invalid(app, client):
    async with client.ws_connect('/history/stream') as ws:
        ws: ClientWebSocketResponse
        await ws.send_json({'empty': True})
        resp = await ws.receive_json(timeout=2)
        assert resp['error']

        await ws.send_str('{not a json obj')
        resp = await ws.receive_json(timeout=2)
        assert resp['error']

        await ws.send_bytes(b'0')
        resp = await ws.receive_json(timeout=2)
        assert resp['error']


async def test_stream_last_values(app, client, query_mock, last_values_result):
    query_mock.side_effect = lambda **kwargs: last_values_result

    async with client.ws_connect('/history/stream') as ws:
        ws: ClientWebSocketResponse
        await ws.send_json({
            'id': 'test',
            'command': 'last_values',
            'query': {
                'measurement': 'm',
                'fields': ['k1', 'k2'],
            },
        })
        resp1 = await ws.receive_json(timeout=5)
        resp2 = await ws.receive_json(timeout=5)
        await ws.send_json({
            'id': 'test',
            'command': 'stop',
        })
        with pytest.raises(asyncio.TimeoutError):
            await ws.receive_json(timeout=0.1)
        assert resp1['data'][0]['field']
        assert resp1 == resp2


async def test_stream_error(app, client, query_mock):
    query_mock.side_effect = lambda **kwargs: RuntimeError()
    async with client.ws_connect('/history/stream') as ws:
        await ws.send_json({
            'id': 'test',
            'command': 'values',
            'query': {
                'measurement': 'm',
                'fields': ['k1', 'k2'],
            },
        })
        with pytest.raises(asyncio.TimeoutError):
            await ws.receive_json(timeout=0.1)


async def test_close_sockets(app, client):
    async with client.ws_connect('/history/stream') as ws:
        ws: ClientWebSocketResponse
        closer = features.get(app, history_api.SocketCloser)
        await closer.before_shutdown(app)
        msg = await ws.receive(1)
        assert msg.type != WSMsgType.TEXT
    await asyncio.sleep(0.1)
