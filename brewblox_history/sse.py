"""
Server-sent events implementation for relaying eventbus messages to front end
"""

import asyncio
import json
from typing import List
from urllib.parse import parse_qs

from aiohttp import hdrs, web
from aiohttp_apispec import docs, querystring_schema
from aiohttp_sse import sse_response
from brewblox_service import brewblox_logger, features, strex

from brewblox_history import influx, queries, schemas

LOGGER = brewblox_logger(__name__)
routes = web.RouteTableDef()

NS_MULT = {
    'ns': 1,
    'u': 1e3,
    'µ': 1e3,
    'ms': 1e6,
    's': 1e9,
    'm': 6e10,
    'h': 3.6e12
}


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


# Manual parsing of query string params, due to an unexplained bug in some clients
def qs_params(qs: str, list_keys: List[str]):
    return {
        k: v if k in list_keys else v[0]
        for k, v in parse_qs(qs, strict_parsing=True).items()
    }


@docs(
    tags=['History'],
    summary='Open an SSE stream for Influx values'
)
@routes.get('/sse/values')
@querystring_schema(schemas.HistorySSEValuesSchema)
async def subscribe_values(request: web.Request) -> web.Response:
    client = influx.get_client(request.app)
    LOGGER.info(f'qs == `{request.query_string}`')
    params = qs_params(request.query_string, ['fields'])
    LOGGER.info(f'params == {params}')
    params = await queries.configure_params(client, **params)
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
                    # to get data updates we adjust the start parameter to result 'time' + 1
                    # 'start' param when given in numbers must always be in ns
                    mult = int(NS_MULT[params.get('epoch') or 'ns'])
                    params['start'] = int(data['values'][-1][0] + 1) * mult
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
    params = qs_params(request.query_string, ['fields'])
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
