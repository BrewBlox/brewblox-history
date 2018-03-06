import asyncio
import collections
import datetime
import logging
from concurrent.futures import CancelledError
from typing import Iterator, Type

from aiohttp import web
from aiohttp.client_exceptions import ClientConnectionError
from aioinflux import InfluxDBClient
from brewblox_service import events

LOGGER = logging.getLogger(__name__)
routes = web.RouteTableDef()

WRITER_KEY = 'influx.writer'
RELAY_KEY = 'influx.relay'
CLIENT_KEY = 'influx.client'
FLAT_SEPARATOR = '/'
WRITE_INTERVAL_S = 1
RECONNECT_INTERVAL_S = 1


def setup(app):
    writer = InfluxWriter(app)
    relay = EventRelay(listener=events.get_listener(app), writer=writer)
    client = QueryClient(app)

    app[WRITER_KEY] = writer
    app[RELAY_KEY] = relay
    app[CLIENT_KEY] = client

    app.router.add_routes(routes)


def get_writer(app) -> 'InfluxWriter':
    return app[WRITER_KEY]


def get_relay(app) -> 'EventRelay':
    return app[RELAY_KEY]


def get_client(app) -> 'QueryClient':
    return app[CLIENT_KEY]


class QueryClient():
    """Offers an InfluxDB client for running queries against.

    No attempt is made to buffer or reschedule queries if the database is unreachable.
    """

    def __init__(self, app: Type[web.Application]=None):
        self._client: InfluxDBClient = None

        if app:
            self.setup(app)

    def setup(self, app: Type[web.Application]):
        app.on_startup.append(self.connect)
        app.on_cleanup.append(self.close)

    async def connect(self, app: Type[web.Application]):
        await self.close()
        self._client = InfluxDBClient(loop=app.loop)

    async def close(self, *args):
        if self._client:
            await self._client.close()
            self._client = None

    async def query(self, database: str, query: str):
        return await self._client.query(query, db=database)


class InfluxWriter():
    """Batch writer of influx data points.

    Offers the write_soon() function where data points can be scheduled for writing to the database.
    Pending data points are flushed to the database every `WRITE_INTERVAL_S` seconds.

    If the database is unreachable when write_soon() is called,
    the data points are kept until the database is available again.
    """

    def __init__(self, app: Type[web.Application]=None, database: str='brewblox'):
        self._pending = []
        self._database = database
        self._task: Type[asyncio.Task] = None
        self._connected = False

        if app:
            self.setup(app)

    def __str__(self):
        return f'<{type(self).__name__} {self._database}>'

    @property
    def is_running(self) -> bool:
        return self._task and not self._task.done()

    def setup(self, app: Type[web.Application]):
        app.on_startup.append(self.start)
        app.on_cleanup.append(self.close)

    async def close(self, *args):
        try:
            if self._task:
                self._task.cancel()
                await self._task
        except CancelledError:
            pass  # We're expecting this one
        except Exception as ex:
            LOGGER.warn(f'Task exception {self} {ex}')

    async def start(self, app: Type[web.Application]):
        await self.close()
        self._task = app.loop.create_task(self._run(app.loop))

    async def _run(self, loop: Type[asyncio.BaseEventLoop]):
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

            except ClientConnectionError as ex:
                LOGGER.warn(f'Database connection lost {self} {ex}')

            except Exception as ex:
                LOGGER.warn(f'Exiting {self} {ex}')
                raise ex

    async def _generate_connections(self, loop: Type[asyncio.BaseEventLoop]) -> Iterator[Type[InfluxDBClient]]:
        """Iterator that keeps yielding new (connected) clients.

        It will only yield an influx client if it could successfully ping the remote.
        There is no limit to total number of clients yielded.
        """
        while True:
            try:
                async with InfluxDBClient(db=self._database, loop=loop) as client:
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
            time=int(now.strftime('%s')),
            measurement=measurement,
            fields=fields,
            tags=tags
        )
        # TODO(Bob): Implement safety valve to prevent memory bloat after database disconnect
        self._pending.append(point)


class EventRelay():
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

    def __init__(self,
                 listener: Type[events.EventListener],
                 writer: Type[InfluxWriter]):
        self._listener = listener
        self._writer = writer

    def subscribe(self, *args, **kwargs):
        """Adds relay behavior to subscription.

        All arguments to this function are passed to brewblox_service.events.subscribe()
        """
        kwargs['on_message'] = self._on_event_message
        self._listener.subscribe(*args, **kwargs)

    def _flatten(self, d, parent_key='', sep='/'):
        items = []
        for k, v in d.items():
            new_key = parent_key + sep + k if parent_key else k
            if isinstance(v, collections.MutableMapping):
                items.extend(self._flatten(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    async def _on_event_message(self,
                                subscription: Type[events.EventSubscription],
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
async def add_subscription(request: Type[web.Request]) -> Type[web.Response]:
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


@routes.post('/query')
async def custom_query(request: Type[web.Request]) -> Type[web.Response]:
    """
    ---
    tags:
    - History
    summary: Query InfluxDB
    description: Send a string query to the database.
    operationId: history.query
    produces:
    - application/json
    parameters:
    -
        in: body
        name: body
        description: Query
        required: true
        schema:
            type: object
            properties:
                database:
                    type: string
                query:
                    type: string
    """
    args = await request.json()
    query_str = args['query']
    database = args.get('database', None)

    data = await get_client(request.app).query(
        query=query_str,
        database=database)
    return web.json_response(data)
