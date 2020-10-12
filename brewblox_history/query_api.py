"""
REST endpoints for queries
"""

import asyncio

from aiohttp import web
from aiohttp.web_ws import WebSocketResponse
from aiohttp_apispec import docs, request_schema
from brewblox_service import brewblox_logger, strex

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


async def ws_receive(ws: WebSocketResponse):
    # We're ignoring all incoming messages for now
    # We still need to keep listening, to catch ping/close messages
    async for msg in ws:
        pass  # pragma: no cover


def _check_open_ended(params: dict) -> bool:
    time_args = [bool(params.get(k)) for k in ('start', 'duration', 'end')]
    return time_args in [
        [False, False, False],
        [True, False, False],
        [False, True, False],
    ]


def _client(request: web.Request) -> influx.QueryClient:
    return influx.fget_client(request.app)


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
    summary='Configure database',
)
@routes.post('/query/configure')
async def configure_db_query(request: web.Request) -> web.Response:
    return web.json_response(
        await configure_db(_client(request), verbose=False)
    )


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


@docs(
    tags=['History'],
    summary='Open a WebSocket to stream values from database as they are added',
)
@routes.get('/query/stream/values')
async def stream_values(request: web.Request) -> web.Response:
    client = _client(request)
    schema = schemas.HistoryStreamedValuesSchema()
    ws = web.WebSocketResponse()

    try:
        await ws.prepare(request)
        params = await ws.receive_json(timeout=10)
        params = schemas.validate(schema, params)
        params = await configure_params(client, **params)
        poll_interval = request.app['config']['poll_interval']
        open_ended = _check_open_ended(params)
        recv_task = asyncio.create_task(ws_receive(ws))

        while not ws.closed:
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

            # Sleep for the poll interval,
            # but wake up early if the websocket is closed
            await asyncio.wait([recv_task], timeout=poll_interval)

    except asyncio.CancelledError:  # pragma: no cover
        raise

    except Exception as ex:
        msg = f'Exiting values stream with error: {strex(ex)}'
        LOGGER.error(msg)
        raise ex

    return ws


@docs(
    tags=['History'],
    summary='Open a WebSocket to periodically get last values for each field',
)
@routes.get('/query/stream/last_values')
async def stream_last_values(request: web.Request) -> web.Response:
    client = _client(request)
    schema = schemas.HistoryLastValuesSchema()
    ws = web.WebSocketResponse()

    try:
        await ws.prepare(request)

        params = await ws.receive_json(timeout=10)
        params = schemas.validate(schema, params)
        poll_interval = request.app['config']['poll_interval']
        recv_task = asyncio.create_task(ws_receive(ws))

        while not ws.closed:
            data = await select_last_values(client, **params)
            await ws.send_json(data)

            # Sleep for the poll interval,
            # but wake up early if the websocket is closed
            await asyncio.wait([recv_task], timeout=poll_interval)

    except asyncio.CancelledError:  # pragma: no cover
        raise

    except Exception as ex:
        msg = f'Exiting last_values stream with error: {strex(ex)}'
        LOGGER.error(msg)
        raise ex

    return ws


def setup(app: web.Application):
    app.router.add_routes(routes)
