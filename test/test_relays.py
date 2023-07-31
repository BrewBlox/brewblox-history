"""
Tests brewblox_history.relays
"""

import asyncio
import json
from unittest.mock import AsyncMock, call

import pytest
from brewblox_service import mqtt, scheduler

from brewblox_history import relays
from brewblox_history.models import HistoryEvent, ServiceConfig

TESTED = relays.__name__


@pytest.fixture
def m_victoria(mocker):
    m = mocker.patch(TESTED + '.victoria.fget').return_value
    m.write = AsyncMock()
    return m


@pytest.fixture
async def setup(app, mocker, m_victoria, mqtt_container):
    config: ServiceConfig = app['config']
    config.mqtt_host = 'localhost'
    config.mqtt_port = mqtt_container['mqtt']

    scheduler.setup(app)
    mqtt.setup(app)
    relays.setup(app)


@pytest.fixture(autouse=True)
async def synchronized(app, client):
    await asyncio.wait_for(mqtt.fget(app).ready.wait(), timeout=5)


async def test_mqtt_relay(app, client, m_victoria):
    topic = 'brewcast/history'
    recv = []
    recv_done = asyncio.Event()

    async def history_cb(topic: str, payload: str):
        recv.append(payload)
        if len(recv) >= 5:
            recv_done.set()

    await mqtt.listen(app, topic, history_cb)

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

    await mqtt.publish(app, topic, json.dumps({'key': 'm', 'data': data}))
    await mqtt.publish(app, topic, json.dumps({'key': 'm', 'data': flat_value}))
    await mqtt.publish(app, topic, json.dumps({'key': 'm', 'data': nested_empty_data}))
    await mqtt.publish(app, topic, json.dumps({'pancakes': 'yummy'}))
    await mqtt.publish(app, topic, json.dumps({'key': 'm', 'data': 'no'}))

    await asyncio.wait_for(recv_done.wait(), timeout=5)

    assert relays.fget(app) is not None
    assert m_victoria.write.call_args_list == [
        call(HistoryEvent(key='m', data=flat_data)),
        call(HistoryEvent(key='m', data=flat_value)),
        call(HistoryEvent(key='m', data={})),
    ]
