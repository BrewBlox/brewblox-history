"""
Master file for pytest fixtures.
Any fixtures declared here are available to all test functions in this directory.
"""


import logging

import pytest
from brewblox_service import service

from brewblox_history import influx
from brewblox_history.__main__ import create_parser


@pytest.fixture(scope='session', autouse=True)
def log_enabled():
    """Sets log level to DEBUG for all test functions.
    Allows all logged messages to be captured during pytest runs"""
    logging.getLogger().setLevel(logging.DEBUG)


@pytest.fixture
def app_config() -> dict:
    return {
        'name': 'test_app',
        'host': 'localhost',
        'port': 1234,
        'debug': False,
        'broadcast_exchange': 'brewcast',
        'logging_exchange': 'logcast',
        'write_interval': 5,
        'poll_interval': 5,
    }


@pytest.fixture
def sys_args(app_config) -> list:
    return [str(v) for v in [
        'app_name',
        '--name', app_config['name'],
        '--host', app_config['host'],
        '--port', app_config['port'],
        '--broadcast-exchange', app_config['broadcast_exchange'],
        '--write-interval', app_config['write_interval'],
        '--poll-interval', app_config['poll_interval'],
    ]]


@pytest.fixture
def app(sys_args):
    parser = create_parser('default')
    app = service.create_app(parser=parser, raw_args=sys_args[1:])
    return app


@pytest.fixture
def client(app, aiohttp_client, loop):
    """Allows patching the app or aiohttp_client before yielding it.

    Any tests wishing to add custom behavior to app can override the fixture
    """
    return loop.run_until_complete(aiohttp_client(app))


@pytest.fixture
def policies_result():
    return {
        'results': [
            {
                'statement_id': 0,
                'series': [
                    {
                        'columns': [
                            'name',
                            'duration',
                            'shardGroupDuration',
                            'replicaN',
                            'default'
                        ],
                        'values': [
                            [
                                'autogen',
                                '24h0m0s',
                                '6h0m0s',
                                1,
                                True
                            ],
                            [
                                'downsample_1m',
                                '0s',
                                '168h0m0s',
                                1,
                                False
                            ],
                            [
                                'downsample_10m',
                                '0s',
                                '168h0m0s',
                                1,
                                False
                            ],
                            [
                                'downsample_1h',
                                '0s',
                                '168h0m0s',
                                1,
                                False
                            ],
                            [
                                'downsample_6h',
                                '0s',
                                '168h0m0s',
                                1,
                                False
                            ]
                        ]
                    }
                ]
            }
        ]
    }


@pytest.fixture
def count_result():
    points_field = influx.COMBINED_POINTS_FIELD
    return {
        'results': [
            {
                'statement_id': 0,
                'series': [
                    {
                        'name': 'brewblox.autogen.pressure',
                        'columns': ['time', f'count_{points_field}'],
                        'values': [[0, 600]],
                    }],
            },
            {
                'statement_id': 1,
                'series': [
                    {
                        'name': 'brewblox.downsample_1m.pressure',
                        'columns': ['time', f'count_m_{points_field}'],
                        'values': [[0, 200]],
                    }],
            },
            {
                'statement_id': 2,
                'series': [
                    {
                        'name': 'brewblox.downsample_10m.pressure',
                        'columns': ['time', f'count_m_m_{points_field}'],
                        'values': [[0, 100]],
                    }],
            },
            {
                'statement_id': 3,
                'series': [
                    {
                        'name': 'brewblox.downsample_1h.pressure',
                        'columns': ['time', f'count_m_m_m_{points_field}'],
                        'values': [[0, 20]],
                    }],
            },
            {
                'statement_id': 4,
                'series': [
                    {
                        'name': 'brewblox.downsample_6h.pressure',
                        'columns': ['time', f'count_m_m_m_m_{points_field}'],
                        'values': [[0, 5]],
                    }],
            },
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
