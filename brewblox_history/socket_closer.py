

from weakref import WeakSet

from aiohttp import WSCloseCode, web
from brewblox_service import features


class SocketCloser(features.ServiceFeature):

    def __init__(self, app: web.Application) -> None:
        super().__init__(app)
        app['websockets'] = WeakSet()

    async def before_shutdown(self, app: web.Application):
        for ws in set(app['websockets']):
            ws: web.WebSocketResponse
            await ws.close(code=WSCloseCode.GOING_AWAY,
                           message='Server shutdown')


def setup(app: web.Application):
    features.add(app, SocketCloser(app))


def fget(app: web.Application) -> SocketCloser:
    return features.get(app, SocketCloser)


def add(app: web.Application, ws: web.WebSocketResponse):
    app['websockets'].add(ws)


def discard(app: web.Application, ws: web.WebSocketResponse):
    app['websockets'].discard(ws)
