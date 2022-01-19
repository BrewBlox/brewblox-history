"""
Functionality for persisting eventbus messages to the database
"""

from contextlib import suppress

from aiohttp import web
from brewblox_service import brewblox_logger, features, mqtt, strex
from pydantic import ValidationError

from brewblox_history import victoria
from brewblox_history.models import HistoryEvent

LOGGER = brewblox_logger(__name__)
routes = web.RouteTableDef()


class MQTTDataRelay(features.ServiceFeature):
    """
    Writes data from subscribed events to the database.

    After a listener is set, it will relay all incoming messages.

    Messages are expected to conform to the following schema:

        {
            'key': str,
            'data': dict,
        }

    The `data` dict is flattened.
    The key name for each field will be a /-separated path to the nested value.

    If we'd received an event where:

        {
            'key': 'controller1',
            'data': {
                'block1': {
                    'sensor1': {
                        settings: {
                            'setting': 'setting'
                        },
                        values: {
                            'value': 'val',
                            'other': 1
                        }
                    }
                }
            }
        }

    `data` would be flattened to:

        {
            'block1/sensor1/settings/setting': 'setting',
            'block1/sensor1/values/value': 'val',
            'block1/sensor1/values/other': 1
        }
    """

    def __init__(self, app):
        super().__init__(app)
        self.topic = None

    def __str__(self):
        return f'<{type(self).__name__} {self.topic}>'

    async def startup(self, app: web.Application):
        self.topic = app['config']['history_topic'] + '/#'
        await mqtt.listen(app, self.topic, self.on_event_message)
        await mqtt.subscribe(app, self.topic)

    async def shutdown(self, app: web.Application):
        with suppress(ValueError):
            await mqtt.unsubscribe(app, self.topic)
        with suppress(ValueError):
            await mqtt.unlisten(app, self.topic, self.on_event_message)

    async def on_event_message(self, topic: str, raw: dict):
        try:
            evt = HistoryEvent(**raw)
            victoria.fget(self.app).write_soon(evt)
            LOGGER.debug(f'MQTT: {evt.key} = {str(evt.data)[:30]}...')

        except ValidationError as ex:
            LOGGER.error(f'Invalid history event: {topic} {strex(ex)}')


def setup(app: web.Application):
    features.add(app, MQTTDataRelay(app))
    app.router.add_routes(routes)


def fget(app: web.Application):
    return features.get(app, MQTTDataRelay)
