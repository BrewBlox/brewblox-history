"""
Master file for pytest fixtures.
Any fixtures declared here are available to all test functions in this directory.
"""


import logging

import pytest
from brewblox_service import service

from brewblox_history import influx
from brewblox_history.__main__ import (controller_error_middleware,
                                       create_parser)


@pytest.fixture(scope='session', autouse=True)
def log_enabled():
    """Sets log level to DEBUG for all test functions.
    Allows all logged messages to be captured during pytest runs"""
    logging.getLogger().setLevel(logging.DEBUG)


@pytest.fixture
def app_config() -> dict:
    return {
        'debug': True,
        'write_interval': 5,
        'poll_interval': 5,
        'influx_host': 'influx',
        'redis_url': 'redis://redis',
        'datastore_topic': 'brewcast/datastore',
    }


@pytest.fixture
def sys_args(app_config) -> list:
    return [str(v) for v in [
        'app_name',
        '--write-interval', app_config['write_interval'],
        '--poll-interval', app_config['poll_interval'],
        '--influx-host', app_config['influx_host'],
        '--redis-url', app_config['redis_url'],
        '--datastore-topic', app_config['datastore_topic'],
        '--debug',
    ]]


@pytest.fixture
def event_loop(loop):
    # aresponses uses the "event_loop" fixture
    # this makes loop available under either name
    yield loop


@pytest.fixture
def app(sys_args):
    parser = create_parser('default')
    app = service.create_app(parser=parser, raw_args=sys_args[1:])
    app.middlewares.append(controller_error_middleware)
    return app


@pytest.fixture
async def client(app, aiohttp_client, loop):
    """Allows patching the app or aiohttp_client before yielding it.

    Any tests wishing to add custom behavior to app can override the fixture
    """
    return await aiohttp_client(app)


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
def measurements_result():
    return {
        'results': [
            {
                'statement_id': 0,
                'series': [
                    {
                        'name': 'measurements',
                        'columns': ['name'],
                        'values': [
                            ['iSpindel000'],
                            ['ispindel'],
                            ['plaato'],
                            ['spark-one'],
                            ['sparkey'],
                            ['spock'],
                            ['testface'],
                            ['vasi-raw']
                        ]
                    }
                ]
            }
        ]
    }


@pytest.fixture
def all_field_keys_result():
    return {
        'results': [
            {
                'statement_id': 0,
                'series': [
                    {
                        'name': 'iSpindel000',
                        'columns': [
                            'fieldKey',
                            'fieldType'
                        ],
                        'values': [
                            [
                                'm_ Combined Influx points',
                                'integer'
                            ],
                            [
                                'm_angle',
                                'float'
                            ],
                            [
                                'm_battery',
                                'float'
                            ]
                        ]
                    }
                ]
            },
            {
                'statement_id': 1,
                'series': [
                    {
                        'name': 'ispindel',
                        'columns': [
                            'fieldKey',
                            'fieldType'
                        ],
                        'values': [
                            [
                                'm_ Combined Influx points',
                                'integer'
                            ],
                            [
                                'm_angle',
                                'float'
                            ],
                            [
                                'm_battery',
                                'float'
                            ]
                        ]
                    }
                ]
            },
            {
                'statement_id': 2,
                'series': [
                    {
                        'name': 'plaato',
                        'columns': [
                            'fieldKey',
                            'fieldType'
                        ],
                        'values': [
                            [
                                'm_ Combined Influx points',
                                'integer'
                            ],
                            [
                                'm_abv',
                                'float'
                            ],
                            [
                                'm_bpm',
                                'float'
                            ]
                        ]
                    }
                ]
            },
            {
                'statement_id': 3,
                'series': [
                    {
                        'name': 'spark-one',
                        'columns': [
                            'fieldKey',
                            'fieldType'
                        ],
                        'values': [
                            [
                                'm_ Combined Influx points',
                                'integer'
                            ],
                            [
                                'm_ActiveGroups/active/0',
                                'float'
                            ],
                            [
                                'm_ActiveGroups/active/1',
                                'float'
                            ],
                            [
                                'm_Case Temp Sensor/value[degC]',
                                'float'
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


@pytest.fixture
def last_values_result():
    return {
        'results': [
            {
                'statement_id': 0,
                'series': [
                    {
                        'name': 'sparkey',
                        'columns': ['time', 'last'],
                        'values': [[1556527890131178000, 0]],
                    },
                ],
            },
            {
                'statement_id': 1,
                'series': [
                    {
                        'name': 'sparkey',
                        'columns': ['time', 'last'],
                        'values': [[1556527890131178000, 100]],
                    },
                ],
            },
            {
                'statement_id': 2,
            },
        ],
    }
