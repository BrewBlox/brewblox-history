from contextlib import asynccontextmanager
from contextvars import ContextVar

from fastapi_mqtt.config import MQTTConfig
from fastapi_mqtt.fastmqtt import FastMQTT

from . import utils

CV: ContextVar[FastMQTT] = ContextVar('mqtt.client')


def setup():
    config = utils.get_config()
    mqtt_config = MQTTConfig(host=config.mqtt_host,
                             port=config.mqtt_port,
                             ssl=(config.mqtt_protocol == 'mqtts'),
                             reconnect_retries=-1)
    fmqtt = FastMQTT(config=mqtt_config)
    CV.set(fmqtt)


@asynccontextmanager
async def lifespan():
    fmqtt = CV.get()
    await fmqtt.connection()
    yield
    await fmqtt.client.disconnect()
