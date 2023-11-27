from contextlib import asynccontextmanager
from contextvars import ContextVar

from fastapi_mqtt.config import MQTTConfig
from fastapi_mqtt.fastmqtt import FastMQTT

CV: ContextVar[FastMQTT] = ContextVar('FastMQTT')


def setup():
    mqtt_config = MQTTConfig(host='eventbus')
    fast_mqtt = FastMQTT(config=mqtt_config)
    CV.set(fast_mqtt)


@asynccontextmanager
async def lifespan():
    fast_mqtt = CV.get()
    await fast_mqtt.connection()
    yield
    await fast_mqtt.client.disconnect()
