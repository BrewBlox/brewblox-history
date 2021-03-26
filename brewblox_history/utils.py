import logging
from weakref import WeakSet

from aiohttp import WSCloseCode, web
from brewblox_service import features


class DuplicateFilter(logging.Filter):
    def filter(self, record):
        current_log = (record.module, record.levelno, record.msg)
        if current_log != getattr(self, 'last_log', None):
            self.last_log = current_log
            return True
        return False


class SocketCloser(features.ServiceFeature):

    def __init__(self, app: web.Application) -> None:
        super().__init__(app)
        app['websockets'] = WeakSet()

    async def startup(self, app: web.Application):
        pass

    async def before_shutdown(self, app: web.Application):
        for ws in set(app['websockets']):
            await ws.close(code=WSCloseCode.GOING_AWAY,
                           message='Server shutdown')

    async def shutdown(self, app: web.Application):
        pass
