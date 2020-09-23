"""
Functionality for persisting eventbus messages to the database
"""

import collections
from contextlib import suppress

from aiohttp import web
from brewblox_service import brewblox_logger, features, mqtt

from brewblox_history import influx, schemas

FLAT_SEPARATOR = '/'

LOGGER = brewblox_logger(__name__)
routes = web.RouteTableDef()


def influx_formatted(d, parent_key='', sep='/'):
    """Converts a (nested) JSON dict to a flat, influx-ready dict

    - Nested values are flattened, using `sep` as path separator
    - Boolean values are converted to a number (0 / 1)
    """
    items = []
    for k, v in d.items():
        new_key = f'{parent_key}{sep}{k}' if parent_key else str(k)

        if isinstance(v, list):
            v = {li: lv for li, lv in enumerate(v)}

        if isinstance(v, collections.MutableMapping):
            items.extend(influx_formatted(v, new_key, sep=sep).items())
        elif isinstance(v, bool):
            items.append((new_key, int(v)))
        else:
            items.append((new_key, v))
    return dict(items)


class MQTTDataRelay(features.ServiceFeature):
    """
    Writes data from subscribed events to the database.

    After a listener is set, it will relay all incoming messages.

    Messages are expected to conform to the following schema:

        {
            'key': str,
            'data': dict,
        }

    'key' becomes the InfluxDB measurement name.
    'data' is flattened into measurement fields.

    Data in sub-dicts is flattened.
    The key name will be the path to the sub-dict, separated by /.

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

    Data would be flattened to:

        {
            'block1/sensor1/settings/setting': 'setting',
            'block1/sensor1/values/value': 'val',
            'block1/sensor1/values/other': 1
        }
    """
    schema = schemas.MQTTHistorySchema(unknown='exclude')

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

    async def on_event_message(self, topic: str, message: dict):
        errors = self.schema.validate(message)
        if errors:
            LOGGER.error(f'Invalid MQTT: {topic} {errors}')
            return

        measurement = message['key']
        data = influx_formatted(message['data'])

        LOGGER.debug(f'MQTT: {measurement} = {str(data)[:30]}...')
        influx.write_soon(self.app, measurement, data)


def setup(app: web.Application):
    features.add(app, MQTTDataRelay(app))
    app.router.add_routes(routes)


def mqtt_relay(app: web.Application):
    return features.get(app, MQTTDataRelay)
