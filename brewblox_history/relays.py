"""
Functionality for persisting eventbus messages to the database
"""

import collections
from datetime import datetime
from typing import Union

from aiohttp import web
from brewblox_service import brewblox_logger, events, features

from brewblox_history import influx

FLAT_SEPARATOR = '/'

LOGGER = brewblox_logger(__name__)
routes = web.RouteTableDef()


def setup(app):
    features.add(app, LogRelay(app))
    features.add(app, DataRelay(app))
    app.router.add_routes(routes)


def get_log_relay(app) -> 'LogRelay':
    return features.get(app, LogRelay)


def get_data_relay(app) -> 'DataRelay':
    return features.get(app, DataRelay)


class LogRelay(features.ServiceFeature):
    """Writes log messages from subscribed events to the database.

    Log messages consist of four parts:
    - Time
    - Origin
    - Category
    - Message

    All values except Time are client-defined string values.
    The only restriction is that they must be valid InfluxDB input.

    When inserting messages as RabbitMQ events, data is split over the routing key, and the message content.
    The routing key should be "<CATEGORY>.<ORIGIN>".
    The message body should be a JSON dict, with the following keys:
    - time (optional): Numeric representation of seconds since UNIX UTC epoch. This can be a float.
    - msg (required): The string message to be logged.
    """

    def __init__(self, app: web.Application):
        super().__init__(app)
        self._listener = events.get_listener(app)
        self._writer = influx.get_log_writer(app)

    def __str__(self):
        return f'<{type(self).__name__} {self._writer}>'

    async def startup(self, _):
        pass

    async def shutdown(self, _):
        pass

    def subscribe(self, *args, **kwargs):
        """Adds relay behavior to subscription.

        All arguments to this function are passed to brewblox_service.events.subscribe()
        """
        kwargs['on_message'] = self._on_event_message
        self._listener.subscribe(*args, **kwargs)

    async def add_log_message(self,
                              timestamp: Union[datetime, str],
                              origin: str,
                              category: str,
                              message: str):
        await self._writer.write_soon(
            measurement=origin,
            time=timestamp,
            fields={
                'msg': message
            },
            tags={
                'category': category
            }
        )

    async def _on_event_message(self,
                                subscription: events.EventSubscription,
                                routing: str,
                                message: dict):
        """Parses log message from event.

        Args:
            subscription (events.EventSubscription):
                Active subscription

            routing (str):
                Event routing. Should consist of logging category and origin.
                Category and origin are expected to be separated by a ".".
                The origin can't contain any more "." characters.

            message (dict):
                Message body. Expected keys:
                    `time` (float, optional): Logging timestamp, expressed as seconds since Unix epoch.
                    `msg`: Log message content.
        """
        category, origin = routing.split('.')
        time = message.get('time')
        time = time and datetime.fromtimestamp(time)

        await self.add_log_message(
            time,
            origin,
            category,
            message['msg']
        )


class DataRelay(features.ServiceFeature):
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

    def __init__(self, app: web.Application):
        super().__init__(app)
        self._listener = events.get_listener(app)
        self._writer = influx.get_data_writer(app)

    def __str__(self):
        return f'<{type(self).__name__} {self._writer}>'

    async def startup(self, _):
        pass

    async def shutdown(self, _):
        pass

    def subscribe(self, *args, **kwargs):
        """Adds relay behavior to subscription.

        All arguments to this function are passed to brewblox_service.events.subscribe()
        """
        kwargs['on_message'] = self._on_event_message
        self._listener.subscribe(*args, **kwargs)

    def _flatten(self, d, parent_key='', sep='/'):
        items = []
        for k, v in d.items():
            new_key = f'{parent_key}{sep}{k}' if parent_key else str(k)

            if isinstance(v, list):
                v = {li: lv for li, lv in enumerate(v)}

            if isinstance(v, collections.MutableMapping):
                items.extend(self._flatten(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    async def _on_event_message(self,
                                subscription: events.EventSubscription,
                                routing: str,
                                message: dict):
        # Routing is formatted as controller name followed by active sub-index
        # A complete push of the controller state is routed as just the controller name
        routing_list = routing.split('.')

        # Convert textual messages to a dict before flattening
        if isinstance(message, str):
            message = dict(text=message)

        parent = FLAT_SEPARATOR.join(routing_list[1:])
        data = self._flatten(message, parent_key=parent, sep=FLAT_SEPARATOR)

        if data:
            await self._writer.write_soon(measurement=routing_list[0], fields=data)


@routes.post('/subscribe')
async def add_subscription(request: web.Request) -> web.Response:
    """
    ---
    tags:
    - History
    summary: Add a new event subscription
    description: All messages matching the subscribed topic will be relayed to the database.
    operationId: history.subscribe
    produces:
    - application/json
    parameters:
    -
        in: body
        name: body
        description: subscription
        required: true
        schema:
            type: object
            properties:
                relay:
                    type: string
                    default: data
                    example: data
                    enum:
                        - log
                        - data
                exchange:
                    type: string
                    example: brewblox
                routing:
                    type: string
                    example: controller.#
    """
    args = await request.json()
    relay_type = args.get('relay', 'data').lower()
    exchange = args['exchange']
    routing = args['routing']

    relay = {
        'data': get_data_relay,
        'log': get_log_relay
    }[relay_type](request.app)

    relay.subscribe(exchange_name=exchange, routing=routing)
    return web.Response()
