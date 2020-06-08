"""
Server-sent events implementation for relaying eventbus messages to front end
"""

import asyncio
import json

from aiohttp import hdrs, web
from aiohttp_apispec import docs, querystring_schema
from aiohttp_sse import sse_response
from brewblox_service import brewblox_logger, features, strex

from brewblox_history import influx, queries, schemas

LOGGER = brewblox_logger(__name__)
routes = web.RouteTableDef()


def setup(app: web.Application):
    features.add(app, ShutdownAlert(app))
    app.router.add_routes(routes)


class ShutdownAlert(features.ServiceFeature):
    def __init__(self, app: web.Application):
        super().__init__(app)
        self._signal: asyncio.Event = None

    @property
    def shutdown_signal(self) -> asyncio.Event:
        return self._signal

    async def startup(self, _):
        self._signal = asyncio.Event()

    async def before_shutdown(self, _):
        self._signal.set()

    async def shutdown(self, _):
        pass


def _check_open_ended(params: dict) -> bool:
    time_args = [bool(params.get(k)) for k in ('start', 'duration', 'end')]
    return time_args in [
        [False, False, False],
        [True, False, False],
        [False, True, False],
    ]


def _cors_headers(request):
    return {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods':
        request.headers.get('Access-Control-Request-Method', ','.join(hdrs.METH_ALL)),
        'Access-Control-Allow-Headers':
        request.headers.get('Access-Control-Request-Headers', '*'),
        'Access-Control-Allow-Credentials': 'true',
    }


@docs(
    tags=['History'],
    summary='Open an SSE stream for Influx values'
)
@routes.get('/sse/values')
@querystring_schema(schemas.HistorySSEValuesSchema)
async def subscribe_values(request: web.Request) -> web.Response:
    client = influx.get_client(request.app)
    params = await queries.configure_params(client, **request['querystring'])
    open_ended = _check_open_ended(params)
    alert: ShutdownAlert = features.get(request.app, ShutdownAlert)
    poll_interval = request.app['config']['poll_interval']

    def check_shutdown():
        if alert.shutdown_signal.is_set():
            raise asyncio.CancelledError()

    async with sse_response(request, headers=_cors_headers(request)) as resp:
        while True:
            try:
                check_shutdown()
                query = queries.build_query(params)
                data = await queries.run_query(client, query, params)

                if data.get('values'):
                    await resp.send(json.dumps(data))
                    # Reset time frame for subsequent updates
                    params['start'] = data['values'][-1][0] + 1
                    params.pop('duration', None)

                if not open_ended:
                    break

                check_shutdown()
                await asyncio.sleep(poll_interval)

            except asyncio.CancelledError:
                return resp

            except Exception as ex:
                msg = f'Exiting values SSE with error: {strex(ex)}'
                LOGGER.error(msg)
                break

    return resp


@docs(
    tags=['History'],
    summary='Open an SSE stream for latest Influx values'
)
@routes.get('/sse/last_values')
@querystring_schema(schemas.HistoryLastValuesSchema)
async def subscribe_last_values(request: web.Request) -> web.Response:
    client = influx.get_client(request.app)
    params = request['querystring']
    alert: ShutdownAlert = features.get(request.app, ShutdownAlert)
    poll_interval = request.app['config']['poll_interval']

    def check_shutdown():
        if alert.shutdown_signal.is_set():
            raise asyncio.CancelledError()

    async with sse_response(request, headers=_cors_headers(request)) as resp:
        while True:
            try:
                check_shutdown()
                data = await queries.select_last_values(client, **params)
                await resp.send(json.dumps(data))

                check_shutdown()
                await asyncio.sleep(poll_interval)

            except asyncio.CancelledError:
                return resp

            except Exception as ex:
                msg = f'Exiting last_values SSE with error: {strex(ex)}'
                LOGGER.error(msg)
                break

    return resp
