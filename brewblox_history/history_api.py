"""
REST endpoints for queries
"""

import asyncio
import json
from contextlib import asynccontextmanager
from weakref import WeakSet

from aiohttp import WSCloseCode, web
from aiohttp_apispec import docs, request_schema
from brewblox_service import brewblox_logger, features, strex

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


def _check_open_ended(params: dict) -> bool:
    time_args = [bool(params.get(k)) for k in ('start', 'duration', 'end')]
    return time_args in [
        [False, False, False],
        [True, False, False],
        [False, True, False],
    ]


def _client(request: web.Request) -> influx.QueryClient:
    return influx.fget_client(request.app)


@asynccontextmanager
async def protected(desc: str):
    try:
        yield

    except asyncio.CancelledError:
        raise

    except Exception as ex:
        LOGGER.debug(f'{desc} error {strex(ex)}')


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


@docs(
    tags=['Debug'],
    summary='Run a manual query against the database',
)
@routes.post('/_debug/query')
@request_schema(schemas.HistoryDebugQuerySchema)
async def custom_query(request: web.Request) -> web.Response:
    return web.json_response(
        await raw_query(_client(request), **request['data'])
    )


@docs(
    tags=['History'],
    summary='Ping the database',
)
@routes.get(r'/{prefix:(history|query)}/ping')
async def ping_query(request: web.Request) -> web.Response:
    await _client(request).ping()
    return web.json_response(
        data={'ok': True},
        headers={
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
            'Expires': '0',
        })


@docs(
    tags=['History'],
    summary='Configure database',
)
@routes.post(r'/{prefix:(history|query)}/configure')
async def configure_db_query(request: web.Request) -> web.Response:
    return web.json_response(
        await configure_db(_client(request), verbose=False)
    )


@docs(
    tags=['History'],
    summary='List available measurements and fields in the database',
)
@routes.post(r'/{prefix:(history|query)}/objects')
@request_schema(schemas.ObjectsQuerySchema)
async def objects_query(request: web.Request) -> web.Response:
    return web.json_response(
        await show_keys(_client(request), **request['data'])
    )


@docs(
    tags=['History'],
    summary='Get values from database',
)
@routes.post(r'/{prefix:(history|query)}/values')
@request_schema(schemas.HistoryQuerySchema)
async def values_query(request: web.Request) -> web.Response:
    return web.json_response(
        await select_values(_client(request), **request['data'])
    )


@docs(
    tags=['History'],
    summary='Get last values from database for each field',
)
@routes.post(r'/{prefix:(history|query)}/last_values')
@request_schema(schemas.HistoryQuerySchema)
async def last_values_query(request: web.Request) -> web.Response:
    return web.json_response(
        await select_last_values(_client(request), **request['data'])
    )


async def _values(app: web.Application, ws: web.WebSocketResponse, id: str, params: dict):
    client = influx.fget_client(app)
    poll_interval = app['config']['poll_interval']
    configured = False
    open_ended = False
    initial = True

    while True:
        async with protected('values query'):
            if not configured:
                params = await configure_params(client, streamed=True, **params)
                open_ended = _check_open_ended(params)
                configured = True

            query = build_query(params)
            data = await run_query(client, query, params)

            if data.get('values'):
                data['initial'] = initial
                await ws.send_json({'id': id, 'data': data})

                # to get data updates we adjust the start parameter to result 'time' + 1
                # 'start' param when given in numbers must always be in ns
                mult = int(NS_MULT[params.get('epoch', 'ns')])
                params['start'] = int(data['values'][-1][0] + 1) * mult
                params.pop('duration', None)
                initial = False

            if not open_ended:
                break

        await asyncio.sleep(poll_interval)


async def _last_values(app: web.Application, ws: web.WebSocketResponse, id: str, params: dict):
    client = influx.fget_client(app)
    poll_interval = app['config']['poll_interval']

    while True:
        async with protected('last_values query'):
            data = await select_last_values(client, **params)
            await ws.send_json({'id': id, 'data': data})
        await asyncio.sleep(poll_interval)


@docs(
    tags=['History'],
    summary='Open a WebSocket to stream values from database as they are added',
)
@routes.get(r'/{prefix:(history|query)}/stream')
async def stream(request: web.Request) -> web.Response:
    app = request.app
    ws = web.WebSocketResponse()
    streams = {}

    try:
        await ws.prepare(request)
        request.app['websockets'].add(ws)
        cmd_schema = schemas.HistoryStreamCommandSchema()
        query_schema = schemas.HistoryQuerySchema()

        async for msg in ws:
            try:
                msg = json.loads(msg.data)
                schemas.validate(cmd_schema, msg)
                cmd = msg['command']
                id = msg['id']
                query = msg.get('query', {})

                existing: asyncio.Task = streams.pop(id, None)
                existing and existing.cancel()

                if cmd == 'values':
                    schemas.validate(query_schema, query)
                    streams[id] = asyncio.create_task(_values(app, ws, id, query))

                elif cmd == 'last_values':
                    schemas.validate(query_schema, query)
                    streams[id] = asyncio.create_task(_last_values(app, ws, id, query))

                elif cmd == 'stop':
                    pass  # We already removed any pre-existing task from streams

                # Marshmallow validates commands
                # This path should never be reached
                else:  # pragma: no cover
                    raise NotImplementedError('Unknown command')

            except asyncio.CancelledError:
                raise

            except Exception as ex:
                LOGGER.debug(f'Stream read error {strex(ex)}')
                await ws.send_json({
                    'error': strex(ex),
                    'message': msg,
                })

    finally:
        request.app['websockets'].discard(ws)
        # Coverage complains about next line -> exit not being covered
        for task in streams.values():  # pragma: no cover
            task.cancel()

    return ws


def setup(app: web.Application):
    app.router.add_routes(routes)
    features.add(app, SocketCloser(app))
