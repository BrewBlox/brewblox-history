"""
Tests brewblox_history.relays
"""

from unittest.mock import AsyncMock, call

import pytest

from brewblox_history import relays
from brewblox_history.models import HistoryEvent

TESTED = relays.__name__


@pytest.fixture
def m_victoria(mocker):
    m = mocker.patch(TESTED + '.victoria.fget').return_value
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
async def app(app, mocker, m_victoria, m_prepare):
    relays.setup(app)
    return app


async def test_mqtt_relay(app, client, m_victoria):
    relay = relays.fget(app)

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
        'nest/ed/values/2': True,
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

    assert m_victoria.write_soon.call_args_list == [
        call(HistoryEvent(key='m', data=flat_data)),
        call(HistoryEvent(key='m', data=flat_value)),
        call(HistoryEvent(key='m', data={})),
    ]
