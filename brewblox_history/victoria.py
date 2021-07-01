import asyncio
from typing import List

from aiohttp import web
from aiohttp.client import ClientSession
from brewblox_service import brewblox_logger, features, http, repeater, strex

from brewblox_history import utils

LOGGER = brewblox_logger(__name__)
MAX_PENDING_POINTS = 10_000


class VictoriaClient(features.ServiceFeature):

    def __init__(self, app: web.Application):
        super().__init__(app)

        config = self.app['config']
        self._host = config['victoria_host']
        self._port = config['victoria_prometheus_port']
        self._address = f'http://{self._host}:{self._port}'
        self._headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }

    async def startup(self, app: web.Application):
        pass

    async def shutdown(self, app: web.Application):
        pass

    async def ping(self):
        url = f'{self._address}/health'
        async with http.session(self.app).get(url) as resp:
            status = await resp.text()
            if status != 'OK':
                raise ConnectionError(f'Database ping returned warning: "{status}"')

    async def _query(self, query: str, url: str, session: ClientSession):
        async with session.post(url,
                                data=query,
                                headers=self._headers) as resp:
            return await resp.json()

    async def fields(self,
                     start: str = '1d',
                     end: str = '',
                     ):
        url = f'{self._address}/api/v1/series'
        session = http.session(self.app)
        query = f'match[]={{__name__!=""}}&start={start}&end={end}'
        result = await self._query(query, url, session)
        output = [
            v['__name__']
            for v in result['data']
        ]
        return output.sort()

    async def ranges(self,
                     fields: List[str],
                     start: str = None,
                     duration: str = None,
                     end: str = None,
                     resolution: str = '10s',
                     ):
        url = f'{self._address}/api/v1/query'
        session = http.session(self.app)
        duration, end = utils.select_timeframe(start, duration, end)
        LOGGER.info(f'{duration} {end}')
        queries = [
            f'query=avg_over_time({{__name__="{f}"}}[{resolution}])[{duration}]&step={resolution}&time={end}'
            for f in fields
        ]
        return await asyncio.gather(*[self._query(q, url, session) for q in queries])

    async def metrics(self, fields: List[str]):
        url = f'{self._address}/api/v1/query'
        session = http.session(self.app)
        # Note: default stale duration for prometheus is 5m
        # Metrics older than this will not show up
        queries = [
            f'query={{__name__="{f}"}}'
            for f in fields
        ]
        return await asyncio.gather(*[self._query(q, url, session) for q in queries])


class VictoriaWriter(repeater.RepeaterFeature):

    def __init__(self, app: web.Application):
        super().__init__(app)

        config = self.app['config']
        self._host = config['victoria_host']
        self._port = config['victoria_opentsdb_port']
        self._url = f'http://{self._host}:{self._port}/api/put'
        self._write_interval = config['write_interval']
        self._last_err = 'init'
        self._pending = []

    def __str__(self):
        return f'<{type(self).__name__}>'

    async def prepare(self):
        """Overrides RepeaterFeature.prepare()"""
        pass

    async def run(self):
        session = http.session(self.app)

        while True:
            await asyncio.sleep(self._write_interval)

            if not self._pending:
                continue

            points = self._pending.copy()
            LOGGER.debug(f'Pushing {len(points)} points to vm database')

            try:
                await session.post(self._url, json=points)

                # Make sure to keep points that were inserted during the write
                self._pending = self._pending[len(points):]

                if self._last_err:
                    LOGGER.info(f'{self} now active')
                    self._last_err = None

            except asyncio.CancelledError:
                raise

            except Exception as ex:
                msg = strex(ex)
                if msg != self._last_err:
                    LOGGER.warn(f'{self} {msg}')
                    self._last_err = msg
                self._avoid_overflow()

    def _avoid_overflow(self):
        # Ensure that a disconnected database does not cause this service to run out of memory
        # To avoid large gaps, the data is downsampled: only every 2nd element is kept
        # Note: using this approach, resolution decreases with age (downsampled more often)
        if len(self._pending) >= MAX_PENDING_POINTS:
            LOGGER.warn('Downsampling pending points...')
            self._pending = self._pending[::2]

    def write_soon(self,
                   service: str,
                   fields: dict):

        timestamp = utils.ms_time()
        points = [
            {
                'metric': f'{service}/{k}',
                'value': float(v),
                'timestamp': timestamp,
            }
            for k, v in fields.items()
            if utils.try_float(v)
        ]

        self._pending.extend(points)


def setup(app):
    features.add(app, VictoriaClient(app))
    features.add(app, VictoriaWriter(app))


def fget_client(app: web.Application) -> VictoriaClient:
    return features.get(app, VictoriaClient)


def fget_writer(app: web.Application) -> VictoriaWriter:
    return features.get(app, VictoriaWriter)


def write_soon(app: web.Application,
               service: str,
               fields: dict):
    fget_writer(app).write_soon(service, fields)
