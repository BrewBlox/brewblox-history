import asyncio
import datetime
import warnings
from typing import List, Union

from aiohttp import web
from aiohttp.client_exceptions import ClientConnectionError
from aioinflux import InfluxDBClient
from brewblox_service import brewblox_logger, features, repeater

LOGGER = brewblox_logger(__name__)


INFLUX_HOST = 'influx'
RECONNECT_INTERVAL_S = 5
MAX_PENDING_POINTS = 5000

DEFAULT_DATABASE = 'brewblox'
DEFAULT_POLICY = 'autogen'
DEFAULT_EPOCH = 'ns'
COMBINED_POINTS_FIELD = ' Combined Influx points'


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

    async def ping(self):
        return await self._client.ping()

    async def query(self, query: str, **kwargs):
        database = kwargs.get('database', DEFAULT_DATABASE)
        epoch = kwargs.get('epoch', DEFAULT_EPOCH)
        return await self._client.query(query.format(**kwargs), db=database, epoch=epoch)


class InfluxWriter(repeater.RepeaterFeature):
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

        self._last_ok = True
        self._pending = []
        self._database = database or DEFAULT_DATABASE
        self._policies = []

    def __str__(self):
        return f'<{type(self).__name__} {self._database}>'

    async def prepare(self):
        """Overrides RepeaterFeature.prepare()"""
        pass

    async def run(self):
        """Overrides RepeaterFeature.run()"""
        write_interval = self.app['config']['write_interval']
        try:
            async with InfluxDBClient(host=INFLUX_HOST, db=self._database) as client:
                await client.ping()
                LOGGER.info(f'Connected {self}')
                await client.create_database(db=self._database)

                while True:
                    await asyncio.sleep(write_interval)

                    if not self._pending:
                        # Do a quick check whether the connection is still alive
                        await client.ping()
                        continue

                    points = self._pending.copy()
                    await client.write(points)
                    LOGGER.debug(f'Pushed {len(points)} points to database')
                    # Make sure to keep points that were inserted during the write
                    self._pending = self._pending[len(points):]

        except ClientConnectionError as ex:
            if self._last_ok:
                LOGGER.warn(f'Database connection failed {self} {ex}')
                self._last_ok = False
            await asyncio.sleep(RECONNECT_INTERVAL_S)
            self._avoid_overflow()

        except asyncio.CancelledError:
            raise

        except Exception:
            await asyncio.sleep(RECONNECT_INTERVAL_S)
            raise

    def _avoid_overflow(self):
        # Ensure that a disconnected influx does not cause this service to run out of memory
        # To avoid large gaps, the data is downsampled: only every 2nd element is kept
        # Note: using this approach, resolution decreases with age (downsampled more often)
        if len(self._pending) >= MAX_PENDING_POINTS:
            warnings.warn('Downsampling pending points...')
            self._pending = self._pending[::2]

    def write_soon(self,
                   measurement: str,
                   fields: dict,
                   tags: dict = None,
                   time: Union[datetime.datetime, str] = None):
        """Schedules a data point for writing.

        Actual writing is done in a timed interval, to batch database writing.
        If the remote is not connected, the data point is kept locally until reconnect.
        """
        if not fields:
            return
        fields[COMBINED_POINTS_FIELD] = 1
        point = dict(
            time=time or datetime.datetime.today(),
            measurement=measurement,
            fields=fields,
            tags=tags or dict()
        )
        self._pending.append(point)


def setup(app):
    features.add(app, QueryClient(app))
    features.add(app, InfluxWriter(app, database=DEFAULT_DATABASE))


def get_client(app: web.Application) -> QueryClient:
    return features.get(app, QueryClient)


def get_data_writer(app: web.Application) -> InfluxWriter:
    return features.get(app, InfluxWriter)


def write_soon(app: web.Application,
               measurement: str,
               fields: dict,
               *args,
               **kwargs):
    get_data_writer(app).write_soon(measurement, fields, *args, **kwargs)
