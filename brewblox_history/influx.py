import asyncio
import collections
import datetime
from concurrent.futures import CancelledError
from typing import Iterator

from aiohttp import web
from aiohttp.client_exceptions import ClientConnectionError
from aioinflux import InfluxDBClient
from brewblox_service import brewblox_logger, events, features

LOGGER = brewblox_logger(__name__)
routes = web.RouteTableDef()

DEFAULT_DATABASE = 'brewblox'
INFLUX_HOST = 'influx'
FLAT_SEPARATOR = '/'
WRITE_INTERVAL_S = 1
RECONNECT_INTERVAL_S = 1
MAX_PENDING_POINTS = 5000


def setup(app):
    features.add(app, InfluxWriter(app))
    features.add(app, EventRelay(app))
    features.add(app, QueryClient(app))

    app.router.add_routes(routes)


def get_writer(app) -> 'InfluxWriter':
    return features.get(app, InfluxWriter)


def get_relay(app) -> 'EventRelay':
    return features.get(app, EventRelay)


def get_client(app) -> 'QueryClient':
    return features.get(app, QueryClient)


class QueryClient(features.ServiceFeature):
    """Offers an InfluxDB client for running queries against.

    No attempt is made to buffer or reschedule queries if the database is unreachable.
    """

    def __init__(self, app: web.Application=None):
        super().__init__(app)
        self._client: InfluxDBClient = None

    async def startup(self, app: web.Application):
        await self.shutdown()
        self._client = InfluxDBClient(host=INFLUX_HOST, loop=app.loop)

    async def shutdown(self, *_):
        if self._client:
            await self._client.close()
            self._client = None

    async def query(self, query: str, database: str=DEFAULT_DATABASE, **kwargs):
        return await self._client.query(query, db=database, **kwargs)


class InfluxWriter(features.ServiceFeature):
    """Batch writer of influx data points.

    Offers the write_soon() function where data points can be scheduled for writing to the database.
    Pending data points are flushed to the database every `WRITE_INTERVAL_S` seconds.

    If the database is unreachable when write_soon() is called,
    the data points are kept until the database is available again.
    """

    def __init__(self, app: web.Application=None, database: str=DEFAULT_DATABASE):
        super().__init__(app)

        self._pending = []
        self._database = database
        self._task: asyncio.Task = None

    def __str__(self):
        return f'<{type(self).__name__} {self._database}>'

    @property
    def is_running(self) -> bool:
        return self._task and not self._task.done()

    async def startup(self, app: web.Application):
        await self.shutdown()
        self._task = app.loop.create_task(self._run(app.loop))

    async def shutdown(self, *_):
        try:
            self._task.cancel()
            await self._task
        except Exception:
            pass
        finally:
            self._task = None

    async def _run(self, loop: asyncio.BaseEventLoop):
        # _generate_connections will keep yielding new connections
        async for client in self._generate_connections(loop):
            try:
                await client.create_database(db=self._database)
                while True:
                    await asyncio.sleep(WRITE_INTERVAL_S)

                    if not self._pending:
                        # Do a quick check whether the connection is still alive
                        await client.ping()
                        continue

                    await client.write(self._pending)
                    LOGGER.info(f'Pushed {len(self._pending)} points to database')
                    self._pending = []

            except CancelledError:
                break

            except ClientConnectionError as ex:
                LOGGER.warn(f'Database connection lost {self} {ex}')
                await asyncio.sleep(RECONNECT_INTERVAL_S)
                continue

            except Exception as ex:
                LOGGER.warn(f'Exiting {self} {ex}')
                raise ex

    async def _generate_connections(self, loop: asyncio.BaseEventLoop) -> Iterator[InfluxDBClient]:
        """Iterator that keeps yielding new (connected) clients.

        It will only yield an influx client if it could successfully ping the remote.
        There is no limit to total number of clients yielded.
        """
        while True:
            try:
                async with InfluxDBClient(host=INFLUX_HOST, db=self._database, loop=loop) as client:
                    await client.ping()
                    LOGGER.info(f'Connected {self}')
                    yield client

            except ClientConnectionError as ex:
                # Sleep and try reconnect
                await asyncio.sleep(RECONNECT_INTERVAL_S)

    async def write_soon(self,
                         measurement: str,
                         fields: dict = dict(),
                         tags: dict = dict()):
        """Schedules a data point for writing.

        Actual writing is done in a timed interval, to batch database writing.
        If the remote is not connected, the data point is kept locally until reconnect.
        """
        now = datetime.datetime.today()
        point = dict(
            time=now,
            measurement=measurement,
            fields=fields,
            tags=tags
        )
        self._pending.append(point)

        # Ensure that a disconnected influx does not cause this service to run out of memory
        # To avoid large gaps, the data is downsampled: only every 2nd element is kept
        # Note: using this approach, resolution decreases with age (downsampled more often)
        if len(self._pending) >= MAX_PENDING_POINTS:
            LOGGER.warn(f'Downsampling pending points...')
            self._pending = self._pending[::2]


class EventRelay(features.ServiceFeature):
    """Writes all data from specified event queues to the database.

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
        super().__init__(None)  # we don't use startup/shutdown functions
        self._listener = events.get_listener(app)
        self._writer = get_writer(app)

    async def startup(self, *_):
        pass

    async def shutdown(self, *_):
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

    get_relay(request.app).subscribe(
        exchange_name=exchange,
        routing=routing)

    return web.Response()
