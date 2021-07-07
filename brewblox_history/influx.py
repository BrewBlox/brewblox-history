import asyncio
import datetime
import warnings
from typing import List, Union

from aiohttp import web
from aiohttp.client_exceptions import ClientConnectionError
from aioinflux import InfluxDBClient, InfluxDBError, InfluxDBWriteError
from brewblox_service import brewblox_logger, features, repeater, strex

LOGGER = brewblox_logger(__name__)


RECONNECT_INTERVAL_S = 5
MAX_PENDING_POINTS = 5000

DATABASE = 'brewblox'
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
        host = app['config']['influx_host']
        self._client = InfluxDBClient(host=host, db=DATABASE)

    async def shutdown(self, *_):
        if self._client:
            await self._client.close()
            self._client = None

    async def ping(self):
        return await self._client.ping()

    async def query(self,
                    query: str,
                    epoch=DEFAULT_EPOCH,
                    **kwargs):
        return await self._client.query(query.format(**kwargs), epoch=epoch)


class InfluxWriter(repeater.RepeaterFeature):
    """Batch writer of influx data points.

    Offers the write_soon() function where data points can be scheduled for writing to the database.
    Pending data points are flushed to the database every `WRITE_INTERVAL_S` seconds.

    If the database is unreachable when write_soon() is called,
    the data points are kept until the database is available again.

    Offers optional downsampling for all measurements in the database.
    """

    def __init__(self, app: web.Application):
        super().__init__(app)

        self._last_err = 'init'
        self._pending = []
        self._policies = []

    def __str__(self):
        return f'<{type(self).__name__} db={DATABASE}>'

    async def run(self):
        """Periodically flush points to the database.

        Points are appended to the buffer by using write_soon().
        run() collects and writes all points every [write_interval].

        Automatic reconnects are handled here.
        Points are only removed from the buffer after the write completed.

        Influx supports partially-valid writes:
        if 1/5 points in a batch is invalid, the other 4 are still written.
        If an InfluxDBWriteError is raised, written points are removed from the buffer.
        """
        write_interval = self.app['config']['write_interval']
        host = self.app['config']['influx_host']
        try:
            async with InfluxDBClient(host=host, db=DATABASE) as client:
                await client.ping()
                await client.create_database(db=DATABASE)

                while True:
                    await asyncio.sleep(write_interval)

                    if not self._pending:
                        # Do a quick check whether the connection is still alive
                        await client.ping()
                        continue

                    points = self._pending.copy()
                    LOGGER.debug(f'Pushing {len(points)} points to database')

                    try:
                        await client.write(points)
                    except InfluxDBWriteError as ex:
                        # Points were either written or invalid
                        # Either way, no need to retry
                        LOGGER.error(strex(ex))

                    # Make sure to keep points that were inserted during the write
                    self._pending = self._pending[len(points):]

                    if self._last_err:
                        LOGGER.info(f'{self} now active')
                        self._last_err = None

        except asyncio.CancelledError:
            raise

        except (ClientConnectionError, InfluxDBError) as ex:
            msg = strex(ex)
            if msg != self._last_err:
                LOGGER.warn(f'{self} {msg}')
                self._last_err = msg
            await asyncio.sleep(RECONNECT_INTERVAL_S)

        except Exception as ex:
            LOGGER.error(strex(ex))
            await asyncio.sleep(RECONNECT_INTERVAL_S)
            raise

        finally:
            self._avoid_overflow()

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
    features.add(app, InfluxWriter(app))


def fget_client(app: web.Application) -> QueryClient:
    return features.get(app, QueryClient)


def fget_writer(app: web.Application) -> InfluxWriter:
    return features.get(app, InfluxWriter)


def write_soon(app: web.Application,
               measurement: str,
               fields: dict,
               *args,
               **kwargs):
    fget_writer(app).write_soon(measurement, fields, *args, **kwargs)
