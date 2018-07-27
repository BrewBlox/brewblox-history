"""
Tests brewblox_history.relays
"""

import time
from datetime import datetime
from unittest.mock import call

import pytest
from asynctest import CoroutineMock

from brewblox_history import relays

TESTED = relays.__name__


@pytest.fixture
def data_writer_mock(mocker):
    call_mock = mocker.patch(TESTED + '.influx.get_data_writer')
    writer_mock = call_mock.return_value
    writer_mock.write_soon = CoroutineMock()
    return writer_mock


@pytest.fixture
def log_writer_mock(mocker):
    call_mock = mocker.patch(TESTED + '.influx.get_log_writer')
    writer_mock = call_mock.return_value
    writer_mock.write_soon = CoroutineMock()
    return writer_mock


@pytest.fixture
def listener_mock(mocker):
    call_mock = mocker.patch(TESTED + '.events.get_listener')
    return call_mock.return_value


@pytest.fixture
async def app(app, mocker, data_writer_mock, log_writer_mock, listener_mock):
    relays.setup(app)
    return app


async def test_subscribe_endpoint(app, client, mocker):
    data_spy = mocker.spy(relays.get_data_relay(app), 'subscribe')
    log_spy = mocker.spy(relays.get_log_relay(app), 'subscribe')

    assert (await client.post('/subscribe', json={
        'exchange': 'brewblox',
        'routing': 'controller.#'
    })).status == 200
    assert data_spy.call_count == 1
    assert log_spy.call_count == 0

    assert (await client.post('/subscribe', json={
        'relay': 'data',
        'exchange': 'brewblox',
        'routing': 'controller.#'
    })).status == 200
    assert data_spy.call_count == 2
    assert log_spy.call_count == 0

    assert (await client.post('/subscribe', json={
        'relay': 'log',
        'exchange': 'brewblox',
        'routing': 'controller.#'
    })).status == 200
    assert data_spy.call_count == 2
    assert log_spy.call_count == 1

    assert (await client.post('/subscribe', json={
        'relay': 'magic',
        'exchange': 'brewblox',
        'routing': 'controller.#'
    })).status == 500
    assert data_spy.call_count == 2
    assert log_spy.call_count == 1


async def test_relay_subscribe(app, client, listener_mock):
    for relay in [
        relays.get_data_relay(app),
        relays.get_log_relay(app)
    ]:
        relay.subscribe('arg', kw='kwarg')
        listener_mock.subscribe.assert_called_once_with(
            'arg', kw='kwarg', on_message=relay._on_event_message)
        listener_mock.reset_mock()


async def test_data_relay(app, client, data_writer_mock):
    relay = relays.get_data_relay(app)

    data = {
        'nest': {
            'ed': {
                'values': [
                    'val',
                    'var'
                ]
            }
        }
    }

    nested_empty_data = {
        'nest': {
            'ed': {
                'empty': {},
                'data': []
            }
        }
    }

    flat_data = {
        'key/nest/ed/values/0': 'val',
        'key/nest/ed/values/1': 'var'
    }

    flat_value = {
        'single/text': 'value'
    }

    await relay._on_event_message(None, 'route.key', data)
    await relay._on_event_message(None, 'route.single', 'value')
    await relay._on_event_message(None, 'route', nested_empty_data)

    assert data_writer_mock.write_soon.call_args_list == [
        call(measurement='route', fields=flat_data),
        call(measurement='route', fields=flat_value)
    ]


async def test_log_relay(app, client, log_writer_mock):
    relay = relays.get_log_relay(app)
    t = time.time()

    await relay._on_event_message(None, 'INFO.source', {'msg': 'hello'})
    await relay._on_event_message(None, 'WARN.source', {'time': t, 'msg': 'world'})

    assert log_writer_mock.write_soon.call_args_list == [
        call(measurement='source', time=None, fields={'msg': 'hello'}, tags={'category': 'INFO'}),
        call(measurement='source', time=datetime.fromtimestamp(t), fields={'msg': 'world'}, tags={'category': 'WARN'})
    ]
