import asyncio
import datetime
from concurrent.futures import CancelledError
from typing import Iterator, Union

from aiohttp import web
from aiohttp.client_exceptions import ClientConnectionError
from aioinflux import InfluxDBClient
from brewblox_service import brewblox_logger, features, scheduler

LOGGER = brewblox_logger(__name__)


INFLUX_HOST = 'influx'
WRITE_INTERVAL_S = 1
RECONNECT_INTERVAL_S = 1
MAX_PENDING_POINTS = 5000

DEFAULT_DATABASE = 'brewblox'
LOG_DATABASE = 'brewblox_logs'

DEFAULT_RETENTION = '1w'
DOWNSAMPLE_INTERVALS = ['10s', '1m', '10m', '1h']
DOWNSAMPLE_RETENTION = ['INF', 'INF', 'INF', 'INF']


def setup(app):
    features.add(app, QueryClient(app))

    features.add(app,
                 InfluxWriter(app, database=DEFAULT_DATABASE),
                 'data_writer'
                 )

    features.add(app,
                 InfluxWriter(app,
                              database=LOG_DATABASE,
                              downsampling=False),
                 'log_writer'
                 )


def get_client(app) -> 'QueryClient':
    return features.get(app, QueryClient)


def get_data_writer(app) -> 'InfluxWriter':
    return features.get(app, InfluxWriter, 'data_writer')


def get_log_writer(app) -> 'InfluxWriter':
    return features.get(app, InfluxWriter, 'log_writer')


class QueryClient(features.ServiceFeature):
    """Offers an InfluxDB client for running queries against.

    No attempt is made to buffer or reschedule queries if the database is unreachable.
    """

    def __init__(self, app: web.Application):
        super().__init__(app)
        self._client: InfluxDBClient = None

    async def startup(self, app: web.Application):
        await self.shutdown()
        self._client = InfluxDBClient(host=INFLUX_HOST, loop=app.loop, db=DEFAULT_DATABASE)

    async def shutdown(self, *_):
        if self._client:
            await self._client.close()
            self._client = None

    async def query(self, query: str, database: str=None, **kwargs):
        database = database or DEFAULT_DATABASE
        return await self._client.query(query, db=database, **kwargs)


class InfluxWriter(features.ServiceFeature):
    """Batch writer of influx data points.

    Offers the write_soon() function where data points can be scheduled for writing to the database.
    Pending data points are flushed to the database every `WRITE_INTERVAL_S` seconds.

    If the database is unreachable when write_soon() is called,
    the data points are kept until the database is available again.

    Offers optional downsampling for all measurements in the database.
    """

    def __init__(self,
                 app: web.Application,
                 database: str=None,
                 retention: str=None,
                 downsampling: bool=True):
        super().__init__(app)

        self._pending = []
        self._database = database or DEFAULT_DATABASE
        self._retention = retention or DEFAULT_RETENTION
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
                         fields: dict=None,
                         tags: dict=None,
                         time: Union[datetime.datetime, str]=None):
        """Schedules a data point for writing.

        Actual writing is done in a timed interval, to batch database writing.
        If the remote is not connected, the data point is kept locally until reconnect.
        """
        point = dict(
            time=time or datetime.datetime.today(),
            measurement=measurement,
            fields=fields or dict(),
            tags=tags or dict()
        )
        self._pending.append(point)

        # Ensure that a disconnected influx does not cause this service to run out of memory
        # To avoid large gaps, the data is downsampled: only every 2nd element is kept
        # Note: using this approach, resolution decreases with age (downsampled more often)
        if len(self._pending) >= MAX_PENDING_POINTS:
            LOGGER.warn(f'Downsampling pending points...')
            self._pending = self._pending[::2]
