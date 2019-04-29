import asyncio
import datetime
import warnings
from concurrent.futures import CancelledError
from typing import Iterator, List, Union

from aiohttp import web
from aiohttp.client_exceptions import ClientConnectionError
from aioinflux import InfluxDBClient
from brewblox_service import brewblox_logger, features, scheduler, strex

LOGGER = brewblox_logger(__name__)


INFLUX_HOST = 'influx'
RECONNECT_INTERVAL_S = 1
MAX_PENDING_POINTS = 5000

DEFAULT_DATABASE = 'brewblox'
DEFAULT_POLICY = 'autogen'
COMBINED_POINTS_FIELD = ' Combined Influx points'


def setup(app):
    features.add(app, QueryClient(app))

    features.add(app,
                 InfluxWriter(app, database=DEFAULT_DATABASE),
                 'data_writer'
                 )


def get_client(app) -> 'QueryClient':
    return features.get(app, QueryClient)


def get_data_writer(app) -> 'InfluxWriter':
    return features.get(app, InfluxWriter, 'data_writer')


class QueryClient(features.ServiceFeature):
    """Offers an InfluxDB client for running queries against.

    No attempt is made to buffer or reschedule queries if the database is unreachable.
    """

    def __init__(self, app: web.Application):
        super().__init__(app)
        self._client: InfluxDBClient = None
        self._policies: List[str] = []

    async def startup(self, app: web.Application):
        await self.shutdown()
        self._client = InfluxDBClient(host=INFLUX_HOST, db=DEFAULT_DATABASE)

    async def shutdown(self, *_):
        if self._client:
            await self._client.close()
            self._client = None

    async def query(self, query: str, **kwargs):
        database = kwargs.get('database', DEFAULT_DATABASE)
        return await self._client.query(query.format(**kwargs), db=database)


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
                 database: str = None):
        super().__init__(app)

        self._pending = []
        self._database = database or DEFAULT_DATABASE
        self._policies = []
        self._task: asyncio.Task = None

    def __str__(self):
        return f'<{type(self).__name__} {self._database}>'

    @property
    def is_running(self) -> bool:
        return self._task and not self._task.done()

    async def startup(self, app: web.Application):
        await self.shutdown()
        self._task = await scheduler.create_task(app, self._run())

    async def shutdown(self, *_):
        await scheduler.cancel_task(self.app, self._task)
        self._task = None

    async def _run(self):
        write_interval = self.app['config']['write_interval']
        # _generate_connections will keep yielding new connections
        async for client in self._generate_connections():
            try:
                await client.create_database(db=self._database)

                while True:
                    await asyncio.sleep(write_interval)

                    if not self._pending:
                        # Do a quick check whether the connection is still alive
                        await client.ping()
                        continue

                    await client.write(self._pending)
                    LOGGER.debug(f'Pushed {len(self._pending)} points to database')
                    self._pending = []

            except CancelledError:
                break

            except ClientConnectionError as ex:
                warnings.warn(f'Database connection lost {self} {ex}')
                await asyncio.sleep(RECONNECT_INTERVAL_S)
                continue

            except Exception as ex:
                warnings.warn(strex(ex))
                await asyncio.sleep(RECONNECT_INTERVAL_S)
                continue

    async def _generate_connections(self) -> Iterator[InfluxDBClient]:
        """Iterator that keeps yielding new (connected) clients.

        It will only yield an influx client if it could successfully ping the remote.
        There is no limit to total number of clients yielded.
        """
        while True:
            try:
                async with InfluxDBClient(host=INFLUX_HOST, db=self._database) as client:
                    await client.ping()
                    LOGGER.info(f'Connected {self}')
                    yield client

            except ClientConnectionError:
                # Sleep and try reconnect
                await asyncio.sleep(RECONNECT_INTERVAL_S)

    async def write_soon(self,
                         measurement: str,
                         fields: dict,
                         tags: dict = None,
                         time: Union[datetime.datetime, str] = None):
        """Schedules a data point for writing.

        Actual writing is done in a timed interval, to batch database writing.
        If the remote is not connected, the data point is kept locally until reconnect.
        """
        fields[COMBINED_POINTS_FIELD] = 1
        point = dict(
            time=time or datetime.datetime.today(),
            measurement=measurement,
            fields=fields,
            tags=tags or dict()
        )
        self._pending.append(point)

        # Ensure that a disconnected influx does not cause this service to run out of memory
        # To avoid large gaps, the data is downsampled: only every 2nd element is kept
        # Note: using this approach, resolution decreases with age (downsampled more often)
        if len(self._pending) >= MAX_PENDING_POINTS:
            warnings.warn(f'Downsampling pending points...')
            self._pending = self._pending[::2]
