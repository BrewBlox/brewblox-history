import asyncio
import time
from typing import List

from aiohttp import web
from brewblox_service import brewblox_logger, features, http, repeater, strex

LOGGER = brewblox_logger(__name__)


def ms_time():
    return time.time_ns() // 1_000_000


class VictoriaClient(features.ServiceFeature):

    async def startup(self, app: web.Application):
        pass

    async def shutdown(self, app: web.Application):
        pass

    async def read(self, match: str):
        pass

    async def write(self, points: List[dict]):
        pass


class VictoriaWriter(repeater.RepeaterFeature):

    def __init__(self, app: web.Application):
        super().__init__(app)

        self._last_err = 'init'
        self._pending = []

    def __str__(self):
        return f'<{type(self).__name__}>'

    async def prepare(self):
        """Overrides RepeaterFeature.prepare()"""
        pass

    async def run(self):
        write_interval = self.app['config']['write_interval']
        host = 'victoria'
        port = 4242
        url = f'http://{host}:{port}/api/put'
        session = http.session(self.app)

        while True:
            await asyncio.sleep(write_interval)

            if not self._pending:
                continue

            points = self._pending.copy()
            LOGGER.debug(f'Pushing {len(points)} points to vm database')

            try:
                await session.post(url, json=points)
            except asyncio.CancelledError:
                raise
            except Exception as ex:
                LOGGER.error(strex(ex))

            self._pending = self._pending[len(points):]

            if self._last_err:
                LOGGER.info(f'{self} now active')
                self._last_err = None

    def write_soon(self,
                   service: str,
                   fields: dict):

        timestamp = ms_time()
        points = [
            {
                'metric': k,
                'value': v,
                'timestamp': timestamp,
                'tags': {'service': service}
            }
            for k, v in fields.items()
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
