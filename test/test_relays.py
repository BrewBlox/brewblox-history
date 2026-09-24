"""
Tests brewblox_history.relays
"""

import asyncio
from contextlib import AsyncExitStack, asynccontextmanager
from unittest.mock import Mock

import pytest
from fastapi import FastAPI
from httpx import AsyncClient
from pytest_mock import MockerFixture

from brewblox_history import models, mqtt, relays, victoria
from brewblox_history.models import ServiceConfig

TESTED = relays.__name__


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with AsyncExitStack() as stack:
        await stack.enter_async_context(mqtt.lifespan())
        yield


@pytest.fixture
def app() -> FastAPI:
    victoria.setup()
    mqtt.setup()
    relays.setup()
    app = FastAPI(lifespan=lifespan)
    return app


@pytest.fixture
def m_write(app: FastAPI, mocker: MockerFixture):
    m = mocker.spy(victoria.CV.get(), 'write')
    return m


async def test_mqtt_relay(client: AsyncClient, config: ServiceConfig, m_write: Mock, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(models, '_refused_fields', set())
    topic = 'brewcast/history'
    recv = []
    recv_done = asyncio.Event()
    mqtt_client = mqtt.CV.get()

    @mqtt_client.subscribe(config.history_topic + '/#')
    async def on_history_message(client, topic, payload, qos, properties):
        recv.append(payload)
        if len(recv) >= 7:
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

    flat_value = {
        'single/text': 'value',
        'single/number': 2,
    }

    # Values that only JSON can carry, and a name the database cannot store
    edge_data = {
        'nan': float('nan'),
        'inf': float('inf'),
        'huge': 10**400,
        'text': '8',
        'q"uote': 1,
    }

    mqtt_client.publish(topic, {'key': 'm', 'data': data})
    mqtt_client.publish(topic, {'key': 'm', 'data': flat_value})
    mqtt_client.publish(topic, {'key': 'm', 'data': flat_value, 'timestamp': 1234})
    mqtt_client.publish(topic, {'key': 'm', 'data': nested_empty_data})
    mqtt_client.publish(topic, {'key': 'm', 'data': edge_data})
    mqtt_client.publish(topic, {'pancakes': 'yummy'})
    mqtt_client.publish(topic, {'key': 'm', 'data': 'no'})

    await asyncio.wait_for(recv_done.wait(), timeout=5)

    written = [c.args[0] for c in m_write.call_args_list]
    assert [(evt.key, evt.data, evt.timestamp) for evt in written] == [
        ('m', {'nest/ed/values/2': 1.0}, None),
        ('m', {'single/number': 2.0}, None),
        ('m', {'single/number': 2.0}, 1234),
        ('m', {}, None),
        ('m', {'text': 8.0}, None),
    ]
    assert models._refused_fields == {'m/q"uote'}
