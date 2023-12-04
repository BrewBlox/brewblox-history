"""
Tests brewblox_history.relays
"""

import asyncio
from contextlib import AsyncExitStack
from unittest.mock import AsyncMock, Mock, call

import pytest
from fastapi import FastAPI
from httpx import AsyncClient

from brewblox_history import mqtt, relays, victoria
from brewblox_history.models import HistoryEvent, ServiceConfig

TESTED = relays.__name__


@pytest.fixture
def m_victoria(mocker):
    m = Mock()
    m.write = AsyncMock()
    victoria.CV.set(m)
    return m


@pytest.fixture
def app(m_victoria):
    mqtt.setup()
    relays.setup()
    app = FastAPI()
    return app


@pytest.fixture
async def lifespan(app):
    async with AsyncExitStack() as stack:
        # Prevents test hangups if the connection fails
        async with asyncio.timeout(5):
            await stack.enter_async_context(mqtt.lifespan())
        yield


async def test_mqtt_relay(client: AsyncClient, config: ServiceConfig, m_victoria: Mock):
    topic = 'brewcast/history'
    recv = []
    recv_done = asyncio.Event()
    mqtt_client = mqtt.CV.get()

    @mqtt_client.subscribe(config.history_topic + '/#')
    async def on_history_message(client, topic, payload, qos, properties):
        recv.append(payload)
        if len(recv) >= 5:
            recv_done.set()

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

    mqtt_client.publish(topic, {'key': 'm', 'data': data})
    mqtt_client.publish(topic, {'key': 'm', 'data': flat_value})
    mqtt_client.publish(topic, {'key': 'm', 'data': nested_empty_data})
    mqtt_client.publish(topic, {'pancakes': 'yummy'})
    mqtt_client.publish(topic, {'key': 'm', 'data': 'no'})

    await asyncio.wait_for(recv_done.wait(), timeout=5)

    assert m_victoria.write.await_args_list == [
        call(HistoryEvent(key='m', data=flat_data)),
        call(HistoryEvent(key='m', data=flat_value)),
        call(HistoryEvent(key='m', data={})),
    ]
