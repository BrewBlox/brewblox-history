"""
Functionality for persisting eventbus messages to the database
"""

import collections

from aiohttp import web

from brewblox_history import influx
from brewblox_service import brewblox_logger, events, features

FLAT_SEPARATOR = '/'

LOGGER = brewblox_logger(__name__)
routes = web.RouteTableDef()


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

    def _influx_formatted(self, d, parent_key='', sep='/'):
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
                items.extend(self._influx_formatted(v, new_key, sep=sep).items())
            elif isinstance(v, bool):
                items.append((new_key, int(v)))
            else:
                items.append((new_key, v))
        return dict(items)

    async def on_event_message(self,
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
        data = self._influx_formatted(message, parent_key=parent, sep=FLAT_SEPARATOR)

        if data:
            await self._writer.write_soon(measurement=routing_list[0], fields=data)


def setup(app: web.Application):
    features.add(app, DataRelay(app))
    app.router.add_routes(routes)


def get_data_relay(app: web.Application) -> DataRelay:
    return features.get(app, DataRelay)


def subscribe(app: web.Application, exchange_name: str, routing: str):
    """
    Subscribe to exchange/routing with the callback being the data relay.
    """
    events.subscribe(app,
                     exchange_name,
                     routing,
                     on_message=get_data_relay(app).on_event_message)


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
                exchange:
                    type: string
                    example: brewblox
                routing:
                    type: string
                    example: controller.#
    """
    args = await request.json()
    exchange = args['exchange']
    routing = args['routing']

    subscribe(request.app, exchange, routing)
    return web.Response()
