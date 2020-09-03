"""
Tests brewblox_history.relays
"""

import pytest
from mock import AsyncMock, call

from brewblox_history import relays

TESTED = relays.__name__


@pytest.fixture
def m_write_soon(mocker):
    m = mocker.patch(TESTED + '.influx.write_soon')
    return m


@pytest.fixture
def m_prepare(mocker):
    mocker.patch(TESTED + '.mqtt.publish', AsyncMock())
    mocker.patch(TESTED + '.mqtt.subscribe', AsyncMock())
    mocker.patch(TESTED + '.mqtt.listen', AsyncMock())
    mocker.patch(TESTED + '.mqtt.unsubscribe', AsyncMock())
    mocker.patch(TESTED + '.mqtt.unlisten', AsyncMock())


@pytest.fixture
def m_subscribe(mocker):
    m = mocker.patch(TESTED + '.subscribe')
    return m


@pytest.fixture
async def app(app, mocker, m_write_soon, m_prepare):
    relays.setup(app)
    return app


async def test_mqtt_relay(app, client, m_write_soon):
    relay = relays.mqtt_relay(app)

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
        'nest/ed/values/0': 'val',
        'nest/ed/values/1': 'var',
        'nest/ed/values/2': 1,
    }

    flat_value = {
        'single/text': 'value',
    }

    topic = 'brewcast/history'
    await relay.on_event_message(topic, {'key': 'm', 'data': data})
    await relay.on_event_message(topic, {'key': 'm', 'data': flat_value})
    await relay.on_event_message(topic, {'key': 'm', 'data': nested_empty_data})
    await relay.on_event_message(topic, {'pancakes': 'yummy'})
    await relay.on_event_message(topic, {'key': 'm', 'data': 'no'})

    assert m_write_soon.call_args_list == [
        call(app, 'm', flat_data),
        call(app, 'm', flat_value),
        call(app, 'm', {}),
    ]
