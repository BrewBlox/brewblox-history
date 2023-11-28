"""
Master file for pytest fixtures.
Any fixtures declared here are available to all test functions in this directory.
"""


import logging
import os
import socket
from contextlib import contextmanager
from subprocess import run

import pytest

from brewblox_history import app

LOGGER = logging.getLogger(__name__)


def find_free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(('0.0.0.0', 0))
    portnum = s.getsockname()[1]
    s.close()
    return portnum


@contextmanager
def docker_container(name: str, ports: dict[str, int], args: list[str]):
    published = {}
    publish_args = []
    for key, src_port in ports.items():
        dest_port = find_free_port()
        published[key] = dest_port
        publish_args.append(f'--publish={dest_port}:{src_port}')

    stop_args = f'docker stop {name}'
    run_args = ' '.join([
        'docker',
        'run',
        '--rm',
        '--detach',
        f'--name={name}',
        *publish_args,
        *args,
    ])

    run(stop_args,
        shell=True,
        text=True,
        capture_output=True)
    run(run_args,
        shell=True,
        check=True,
        text=True,
        capture_output=True)
    try:
        yield published
    finally:
        run(stop_args,
            shell=True,
            text=True,
            capture_output=True)


@pytest.fixture(scope='session', autouse=True)
def env_settings():
    os.environ['BREWBLOX_DEBUG'] = 'True'


@pytest.fixture(scope='session', autouse=True)
def log_enabled(env_settings):
    app.init_logging()


@pytest.fixture(scope='session')
def mqtt_container():
    with docker_container(
        name='mqtt-test-container',
        ports={'mqtt': 1883, 'ws': 15675},
        args=['ghcr.io/brewblox/mosquitto:develop'],
    ) as ports:
        yield ports


@pytest.fixture(scope='session')
def redis_container():
    with docker_container(
        name='redis-test-container',
        ports={'redis': 6379},
        args=['redis:6.0'],
    ) as ports:
        yield ports
