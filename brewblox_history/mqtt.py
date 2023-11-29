import asyncio
from contextlib import asynccontextmanager
from contextvars import ContextVar

from fastapi_mqtt.config import MQTTConfig
from fastapi_mqtt.fastmqtt import FastMQTT

from . import utils

CV: ContextVar[FastMQTT] = ContextVar('mqtt.client')
CV_CONNECTED: ContextVar[asyncio.Event] = ContextVar('mqtt.connected')


def setup():
    config = utils.get_config()
    mqtt_config = MQTTConfig(host=config.mqtt_host,
                             port=config.mqtt_port,
                             ssl=(config.mqtt_protocol == 'mqtts'),
                             reconnect_retries=-1)
    fast_mqtt = FastMQTT(config=mqtt_config)
    evt = asyncio.Event()
    CV.set(fast_mqtt)
    CV_CONNECTED.set(evt)

    @fast_mqtt.on_connect()
    def on_connect(client, flags, rc, properties):
        CV_CONNECTED.get().set()

    @fast_mqtt.on_disconnect()
    def on_disconnect(client, packet, exc=None):
        CV_CONNECTED.get().clear()


@asynccontextmanager
async def lifespan():
    fast_mqtt = CV.get()
    await fast_mqtt.connection()
    yield
    await fast_mqtt.client.disconnect()
