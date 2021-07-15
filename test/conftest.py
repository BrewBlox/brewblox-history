"""
Master file for pytest fixtures.
Any fixtures declared here are available to all test functions in this directory.
"""


import logging

import pytest
from brewblox_service import service

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
        'write_interval': 30,
        'ranges_interval': 30,
        'metrics_interval': 5,
        'victoria_url': 'http://victoria:8428',
        'redis_url': 'redis://redis',
        'datastore_topic': 'brewcast/datastore',
    }


@pytest.fixture
def sys_args(app_config) -> list:
    return [str(v) for v in [
        'app_name',
        '--write-interval', app_config['write_interval'],
        '--ranges-interval', app_config['ranges_interval'],
        '--metrics-interval', app_config['metrics_interval'],
        '--victoria-url', app_config['victoria_url'],
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
