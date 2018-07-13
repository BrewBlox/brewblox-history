import asyncio
import collections
import datetime
from concurrent.futures import CancelledError
from typing import Iterator

from aiohttp import web
from aiohttp.client_exceptions import ClientConnectionError
from aioinflux import InfluxDBClient
from brewblox_service import brewblox_logger, events, features, scheduler

LOGGER = brewblox_logger(__name__)
routes = web.RouteTableDef()


INFLUX_HOST = 'influx'
FLAT_SEPARATOR = '/'
WRITE_INTERVAL_S = 1
RECONNECT_INTERVAL_S = 1
MAX_PENDING_POINTS = 5000

DEFAULT_DATABASE = 'brewblox'
DEFAULT_RETENTION = '1w'

DOWNSAMPLE_INTERVALS = ['10s', '1m', '10m', '1h']
DOWNSAMPLE_RETENTION = ['INF', 'INF', 'INF', 'INF']


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

    def __init__(self,
                 app: web.Application=None,
                 database: str=DEFAULT_DATABASE,
                 retention: str=DEFAULT_RETENTION,
                 downsampling: bool=True):
        super().__init__(app)

        self._pending = []
        self._database = database
        self._retention = retention
        self._downsampling = downsampling
        self._task: asyncio.Task = None
        self._skip_config = False

    def __str__(self):
        return f'<{type(self).__name__} {self._database}>'

    @property
    def is_running(self) -> bool:
        return self._task and not self._task.done()

    async def startup(self, app: web.Application):
        await self.shutdown()
        self._skip_config = app['config']['skip_influx_config']
        self._task = await scheduler.create_task(app, self._run())

    async def shutdown(self, *_):
        await scheduler.cancel_task(self.app, self._task)
        self._task = None

    async def _on_connected(self, client: InfluxDBClient):
        """
        Sets database configuration.
        This is done every time a new connection is made.
        """

        if self._skip_config:
            return

        # Creates default database, and limits retention
        await client.create_database(db=self._database)
        await client.query(f'ALTER RETENTION POLICY autogen ON {self._database} duration {self._retention}')

        if self._downsampling:
            # Data is downsampled multiple times, and stored in separate databases
            # Database naming scheme is <database_name>_<downsample_interval>
            for interval, retention in zip(DOWNSAMPLE_INTERVALS, DOWNSAMPLE_RETENTION):
                downsampled_db = f'{self._database}_{interval}'
                db_query = f'CREATE DATABASE {downsampled_db} WITH DURATION {retention}'
                cquery = f'''
                CREATE CONTINUOUS QUERY "cq_downsample_{interval}" ON "{self._database}"
                BEGIN
                    SELECT mean(*) INTO "{downsampled_db}"."autogen".:MEASUREMENT
                    FROM /.*/
                    GROUP BY time({interval}),*
                END
                '''
                await client.query(db_query)
                await client.query(cquery)

    async def _run(self):
        # _generate_connections will keep yielding new connections
        async for client in self._generate_connections():
            try:
                await self._on_connected(client)

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

    async def _generate_connections(self) -> Iterator[InfluxDBClient]:
        """Iterator that keeps yielding new (connected) clients.

        It will only yield an influx client if it could successfully ping the remote.
        There is no limit to total number of clients yielded.
        """
        while True:
            try:
                async with InfluxDBClient(host=INFLUX_HOST, db=self._database, loop=self.app.loop) as client:
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
        super().__init__(app, startup=features.Startup.MANUAL)
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
