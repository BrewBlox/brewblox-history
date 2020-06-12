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
    mocker.patch(TESTED + '.amqp.subscribe')
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


async def test_amqp_relay(app, client, m_write_soon):
    relay = relays.amqp_relay(app)

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

    assert m_write_soon.call_args_list == [
        call(app, 'route', flat_data),
        call(app, 'route', flat_value),
        call(app, 'route', {}),
    ]


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

    assert m_write_soon.call_args_list == [
        call(app, 'm', flat_data),
        call(app, 'm', flat_value),
        call(app, 'm', {}),
    ]


async def test_retained_relay(app, client):
    relay = relays.retained_relay(app)
    m_publish = relays.mqtt.publish

    await relay.on_request_message('brewcast/request/state', {})
    m_publish.assert_not_awaited()

    msg_1_false = {
        'key': 'value_service',
        'type': 'value',
        'data': {'value': False}
    }
    msg_1_true = {
        'key': 'value_service',
        'type': 'value',
        'data': {'value': True}
    }
    msg_2_false = {
        'key': 'other_service',
        'type': 'other',
        'data': [False]
    }
    msg_2_true = {
        'key': 'other_service',
        'type': 'other',
        'data': [True]
    }
    msg_3_invalid = {
        'key': 'fantasy',
        'type': 'emptiness',
        'data': 'nonsense',
    }
    msg_cancelled = {
        'key': 'cancelled',
        'type': 'testing',
        'data': {'soon': True},
    }

    await relay.on_state_message('brewcast/state', msg_1_false)
    await relay.on_state_message('brewcast/state', msg_1_true)
    await relay.on_state_message('brewcast/state', {'key': 'testface'})
    await relay.on_state_message('brewcast/state', msg_2_false)
    await relay.on_state_message('brewcast/state/other', msg_2_true)
    await relay.on_state_message('brewcast/state/invalid', msg_3_invalid)
    await relay.on_state_message('brewcast/cancelled', msg_cancelled)
    await relay.on_state_message('brewcast/cancelled', None)
    await relay.on_state_message('brewcast', None)

    await relay.on_request_message('brewcast/request/state', {})

    m_publish.assert_has_awaits([
        call(app, 'brewcast/state', msg_1_true),
        call(app, 'brewcast/state/other', msg_2_true),
    ], any_order=True)
