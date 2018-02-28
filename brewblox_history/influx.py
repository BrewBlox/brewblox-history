import asyncio
import collections
import datetime
import logging
from typing import Type

from aiohttp import web
from aiohttp.client_exceptions import ClientConnectionError
from aioinflux import AsyncInfluxDBClient

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
    relay = EventRelay(app,
                       listener=events.get_listener(app),
                       writer=writer)

    app[WRITER_KEY] = writer
    app[RELAY_KEY] = relay
    app[CLIENT_KEY] = QueryClient(app)

    app.router.add_routes(routes)


def get_writer(app) -> 'InfluxWriter':
    return app[WRITER_KEY]


def get_relay(app) -> 'EventRelay':
    return app[RELAY_KEY]


def get_client(app) -> 'QueryClient':
    return app[CLIENT_KEY]


class QueryClient():

    def __init__(self, app: Type[web.Application]=None):
        self._client: AsyncInfluxDBClient = None

        if app:
            self.setup(app)

    def setup(self, app: Type[web.Application]):
        app.on_startup.append(self.connect)
        app.on_cleanup.append(self.close)

    async def connect(self, app: Type[web.Application]):
        await self.close()
        self._client = AsyncInfluxDBClient(loop=app.loop)

    async def close(self, *args):
        if self._client:
            await self._client.close()
            self._client = None

    async def query(self, database: str, query: str):
        return await self._client.query(query, db=database)


class InfluxWriter():

    def __init__(self, app: Type[web.Application]=None, database='brewblox'):
        self._pending = []
        self._database = database
        self._task = None
        self._connected = False

        if app:
            self.setup(app)

    def __str__(self):
        return f'<{type(self).__name__} {self._database}>'

    async def _startup(self, app: Type[web.Application]):
        await self.start(app.loop)

    async def _cleanup(self, app: Type[web.Application]):
        await self.close()

    def setup(self, app: Type[web.Application]):
        app.on_startup.append(self._startup)
        app.on_cleanup.append(self._cleanup)

    async def close(self):
        try:
            if self._task:
                self._task.cancel()
                await self._task
        except Exception:
            pass

    async def start(self, loop: Type[asyncio.BaseEventLoop]):
        self._task = loop.create_task(self._run(loop))

    async def _run(self, loop: Type[asyncio.BaseEventLoop]):
        # _reconnect will keep yielding new connections
        async for client in self._reconnect(loop):
            try:
                await client.create_database(db=self._database)
                while True:
                    await asyncio.sleep(WRITE_INTERVAL_S)
                    # Do a quick check whether the connection is still alive
                    await client.ping()

                    if not self._pending:
                        continue

                    await client.write(self._pending)
                    LOGGER.info(f'Pushed {len(self._pending)} points to database')
                    del self._pending[:]

            except ClientConnectionError as ex:
                LOGGER.warn(f'Database connection lost {self} {ex}')

            except Exception as ex:
                LOGGER.warn(f'Exiting {self} {ex}')
                raise ex

    async def _reconnect(self, loop: Type[asyncio.BaseEventLoop]):
        while True:
            try:
                async with AsyncInfluxDBClient(db=self._database, loop=loop) as client:
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

        now = datetime.datetime.today()
        point = dict(
            time=int(now.strftime('%s')),
            measurement=measurement,
            fields=fields,
            tags=tags
        )
        self._pending.append(point)


class EventRelay():
    """Writes all data from specified event queues to the database.

    TODO(Bob): Specifics
    """

    def __init__(self,
                 app: Type[web.Application]=None,
                 listener: Type[events.EventListener]=None,
                 writer: Type[InfluxWriter]=None):
        self._writer = writer if writer else InfluxWriter(app)
        self._listener = listener if listener else events.EventListener(app)

    def subscribe(self, *args, **kwargs):
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

        if isinstance(message, dict):
            parent = FLAT_SEPARATOR.join(routing_list[1:])
            data = self._flatten(message, parent_key=parent, sep=FLAT_SEPARATOR)
        else:
            data = dict(val=message)

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
