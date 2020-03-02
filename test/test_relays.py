"""
Tests brewblox_history.relays
"""

from unittest.mock import AsyncMock, call

import pytest

from brewblox_history import relays
from brewblox_service.testing import response

TESTED = relays.__name__


@pytest.fixture
def m_data_writer(mocker):
    call_mock = mocker.patch(TESTED + '.influx.get_data_writer')
    writer_mock = call_mock.return_value
    writer_mock.write_soon = AsyncMock()
    return writer_mock


@pytest.fixture
def m_listener(mocker):
    call_mock = mocker.patch(TESTED + '.events.get_listener')
    return call_mock.return_value


@pytest.fixture
def m_subscribe(mocker):
    m = mocker.patch(TESTED + '.subscribe')
    return m


@pytest.fixture
async def app(app, mocker, m_data_writer, m_listener):
    relays.setup(app)
    return app


async def test_subscribe_endpoint(app, client, m_subscribe):
    await response(client.post('/subscribe', json={
        'exchange': 'brewblox',
        'routing': 'controller.#'
    }))
    assert m_subscribe.call_count == 1


async def test_data_relay(app, client, m_data_writer):
    relay = relays.get_data_relay(app)

    data = {
        'nest': {
            'ed': {
                'values': [
                    'val',
                    'var',
                    True,
                ]
            }
        }
    }

    nested_empty_data = {
        'nest': {
            'ed': {
                'empty': {},
                'data': [],
            }
        }
    }

    flat_data = {
        'key/nest/ed/values/0': 'val',
        'key/nest/ed/values/1': 'var',
        'key/nest/ed/values/2': 1,
    }

    flat_value = {
        'single/text': 'value',
    }

    await relay.on_event_message(None, 'route.key', data)
    await relay.on_event_message(None, 'route.single', 'value')
    await relay.on_event_message(None, 'route', nested_empty_data)

    assert m_data_writer.write_soon.call_args_list == [
        call(measurement='route', fields=flat_data),
        call(measurement='route', fields=flat_value)
    ]
