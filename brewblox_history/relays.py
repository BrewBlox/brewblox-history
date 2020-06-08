"""
Functionality for persisting eventbus messages to the database
"""

import asyncio
import collections
from contextlib import suppress

from aiohttp import web
from brewblox_service import brewblox_logger, features, mqtt

from brewblox_history import amqp, influx, schemas

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


class AMQPDataRelay(features.ServiceFeature):
    """Writes data from subscribed events to the database.

    After a subscription is set, it will relay all incoming messages.

    When relaying, the data dict is flattened.
    The first part of the routing key is considered the controller name,
    and becomes the InfluxDB measurement name.

    All subsequent routing key components are considered to be sub-set indicators of the controller.
    If the routing key is controller1.block1.sensor1, we consider this as being equal to:

        'controller1': {
            'block1': {
                'sensor1': <event data>
            }
        }

    Data in sub-dicts (including those implied by routing key) is flattened.
    The key name will be the path to the sub-dict, separated by /.

    If we'd received an event where:

        routing_key = 'controller1.block1.sensor1'
        data = {
            settings: {
                'setting': 'setting'
            },
            values: {
                'value': 'val',
                'other': 1
            }
        }

    it would be flattened to:

        {
            'block1/sensor1/settings/setting': 'setting',
            'block1/sensor1/values/value': 'val',
            'block1/sensor1/values/other': 1
        }

    If the event data is not a dict, but a string, it is first converted to:

        {
            'text': <string data>
        }

    This dict is then flattened.
    """

    def __init__(self, app):
        super().__init__(app)
        self.exchange = None

    def __str__(self):
        return f'<{type(self).__name__} {self.exchange}>'

    async def startup(self, app: web.Application):
        self.exchange = app['config']['broadcast_exchange']
        amqp.subscribe(app,
                       exchange_name=self.exchange,
                       routing='#',
                       on_message=self.on_event_message)

    async def shutdown(self, _):
        pass

    async def on_event_message(self,
                               subscription: amqp.EventSubscription,
                               routing: str,
                               message: dict):
        # Routing is formatted as controller name followed by active sub-index
        # A complete push of the controller state is routed as just the controller name
        routing_list = routing.split('.')

        # Convert textual messages to a dict before flattening
        if isinstance(message, str):
            message = dict(text=message)

        measurement = routing_list[0]
        parent = FLAT_SEPARATOR.join(routing_list[1:])
        data = influx_formatted(message, parent_key=parent, sep=FLAT_SEPARATOR)

        LOGGER.debug(f'recv {measurement}, data={bool(data)}')
        influx.write_soon(self.app, measurement, data)


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


class MQTTRetainedRelay(features.ServiceFeature):

    schema = schemas.MQTTStateSchema(unknown='exclude')

    def __init__(self, app):
        super().__init__(app)
        self.state_topic = None
        self.request_topic = 'brewcast/request/state'
        self.cache = {}

    async def startup(self, app: web.Application):
        self.state_topic = app['config']['state_topic'] + '/#'
        await mqtt.listen(app, self.state_topic, self.on_state_message)
        await mqtt.subscribe(app, self.state_topic)
        await mqtt.subscribe(app, self.request_topic)
        await mqtt.listen(app, self.request_topic, self.on_request_message)

    async def shutdown(self, app: web.Application):
        with suppress(ValueError):
            await mqtt.unsubscribe(app, self.state_topic)
            await mqtt.unsubscribe(app, self.request_topic)
            await mqtt.unlisten(app, self.state_topic, self.on_state_message)
            await mqtt.unlisten(app, self.request_topic, self.on_request_message)

    async def on_state_message(self, topic: str, message: dict):
        errors = self.schema.validate(message)
        if errors:
            LOGGER.error(f'Invalid State message: {topic} {errors}')
            return

        key = message['key']
        type = message['type']
        self.cache[f'{key}__{type}'] = (topic, message)

    async def on_request_message(self, topic: str, message: dict):
        LOGGER.info(f'Cached: {[*self.cache]}')
        if self.cache:
            await asyncio.gather(
                *[mqtt.publish(self.app, topic, message)
                  for (topic, message) in self.cache.values()],
                return_exceptions=True
            )


def setup(app: web.Application):
    features.add(app, AMQPDataRelay(app))
    features.add(app, MQTTDataRelay(app))
    features.add(app, MQTTRetainedRelay(app))
    app.router.add_routes(routes)


def amqp_relay(app: web.Application):
    return features.get(app, AMQPDataRelay)


def mqtt_relay(app: web.Application):
    return features.get(app, MQTTDataRelay)


def retained_relay(app: web.Application):
    return features.get(app, MQTTRetainedRelay)
