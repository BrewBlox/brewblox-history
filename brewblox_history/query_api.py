"""
REST endpoints for queries
"""

import asyncio

from aiohttp import web
from aiohttp_apispec import docs, request_schema
from brewblox_service import brewblox_logger, features, strex
from marshmallow import ValidationError

from brewblox_history import influx, schemas
from brewblox_history.queries import (build_query, configure_db,
                                      configure_params, raw_query, run_query,
                                      select_last_values, select_values,
                                      show_keys)

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


def _client(request: web.Request) -> influx.QueryClient:
    return influx.get_client(request.app)


async def check_shutdown(app: web.Application):
    if features.get(app, ShutdownAlert).shutdown_signal.is_set():
        raise asyncio.CancelledError()


@docs(
    tags=['Debug'],
    summary='Run a manual query against the database',
)
@routes.post('/_debug/query')
@request_schema(schemas.DebugQuerySchema)
async def custom_query(request: web.Request) -> web.Response:
    return web.json_response(
        await raw_query(_client(request), **request['data'])
    )


@docs(
    tags=['Debug'],
    summary='Ping the database',
)
@routes.get('/ping')
async def ping(request: web.Request) -> web.Response:
    await _client(request).ping()
    return web.json_response({'ok': True})


@docs(
    tags=['History'],
    summary='List available measurements and fields in the database',
)
@routes.post('/query/objects')
@request_schema(schemas.ObjectsQuerySchema)
async def objects_query(request: web.Request) -> web.Response:
    return web.json_response(
        await show_keys(_client(request), **request['data'])
    )


@docs(
    tags=['History'],
    summary='Get values from database',
)
@routes.post('/query/values')
@request_schema(schemas.HistoryValuesSchema)
async def values_query(request: web.Request) -> web.Response:
    return web.json_response(
        await select_values(_client(request), **request['data'])
    )


@routes.get('/query/stream/values')
async def stream_values(request: web.Request) -> web.Response:
    client = _client(request)
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    async for msg in ws:
        params = msg.json()
        errors = schemas.HistorySSEValuesSchema().validate(params)
        if errors:
            LOGGER.error(f'Invalid values request: {errors}')
            raise ValidationError(errors)

        params = await configure_params(client, **params)
        open_ended = _check_open_ended(params)
        poll_interval = request.app['config']['poll_interval']

        while True:
            try:
                await check_shutdown(request.app)
                query = build_query(params)
                data = await run_query(client, query, params)

                if data.get('values'):
                    await ws.send_json(data)
                    # to get data updates we adjust the start parameter to result 'time' + 1
                    # 'start' param when given in numbers must always be in ns
                    mult = int(NS_MULT[params.get('epoch') or 'ns'])
                    params['start'] = int(data['values'][-1][0] + 1) * mult
                    params.pop('duration', None)

                if not open_ended:
                    break

                await check_shutdown(request.app)
                await asyncio.sleep(poll_interval)

            except asyncio.CancelledError:
                return ws

            except Exception as ex:
                msg = f'Exiting values stream with error: {strex(ex)}'
                LOGGER.error(msg)
                raise ex

    return ws


@docs(
    tags=['History'],
    summary='Get last values from database for each field',
)
@routes.post('/query/last_values')
@request_schema(schemas.HistoryLastValuesSchema)
async def last_values_query(request: web.Request) -> web.Response:
    return web.json_response(
        await select_last_values(_client(request), **request['data'])
    )


@routes.get('/query/stream/last_values')
async def stream_last_values(request: web.Request) -> web.Response:
    client = _client(request)
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    async for msg in ws:
        params = msg.json()
        errors = schemas.HistoryLastValuesSchema().validate(params)
        if errors:
            LOGGER.error(f'Invalid values request: {errors}')
            raise ValidationError(errors)

        poll_interval = request.app['config']['poll_interval']

        while True:
            try:
                await check_shutdown(request.app)
                data = await select_last_values(client, **params)
                await ws.send_json(data)

                await check_shutdown(request.app)
                await asyncio.sleep(poll_interval)

            except asyncio.CancelledError:
                return ws

            except Exception as ex:
                msg = f'Exiting last_values stream with error: {strex(ex)}'
                LOGGER.error(msg)
                raise ex

    return ws


@docs(
    tags=['History'],
    summary='Configure database',
)
@routes.post('/query/configure')
async def configure_db_query(request: web.Request) -> web.Response:
    return web.json_response(
        await configure_db(_client(request), verbose=False)
    )


def setup(app: web.Application):
    features.add(app, ShutdownAlert(app))
    app.router.add_routes(routes)
