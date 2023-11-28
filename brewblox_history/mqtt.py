from contextlib import asynccontextmanager
from contextvars import ContextVar

from fastapi_mqtt.config import MQTTConfig
from fastapi_mqtt.fastmqtt import FastMQTT

from .models import ServiceConfig

CV: ContextVar[FastMQTT] = ContextVar('FastMQTT')


def setup():
    config = ServiceConfig.cached()
    mqtt_config = MQTTConfig(host=config.mqtt_host,
                             port=config.mqtt_port,
                             reconnect_retries=-1)
    fast_mqtt = FastMQTT(config=mqtt_config)
    CV.set(fast_mqtt)


@asynccontextmanager
async def lifespan():
    fast_mqtt = CV.get()
    await fast_mqtt.connection()
    yield
    await fast_mqtt.client.disconnect()
