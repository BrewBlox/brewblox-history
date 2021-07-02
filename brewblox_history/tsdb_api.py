"""
REST endpoints for TSDB queries
"""

import asyncio
import json
from contextlib import asynccontextmanager

from aiohttp import web
from aiohttp_apispec import docs, request_schema
from brewblox_service import brewblox_logger, strex

from brewblox_history import schemas, utils, victoria

LOGGER = brewblox_logger(__name__)
routes = web.RouteTableDef()

LOGGER.addFilter(utils.DuplicateFilter())


def _check_open_ended(params: dict) -> bool:
    time_args = [bool(params.get(k)) for k in ('start', 'duration', 'end')]
    return time_args in [
        [False, False, False],
        [True, False, False],
        [False, True, False],
    ]


def _client(request: web.Request) -> victoria.VictoriaClient:
    return victoria.fget_client(request.app)


@asynccontextmanager
async def protected(desc: str):
    try:
        yield

    except asyncio.CancelledError:
        raise

    except Exception as ex:
        LOGGER.error(f'{desc} error {strex(ex)}')


@docs(
    tags=['TSDB'],
    summary='Ping the database',
)
@routes.get('/tsdb/ping')
async def ping_endpoint(request: web.Request) -> web.Response:
    await _client(request).ping()
    return web.json_response(
        data={'ok': True},
        headers={
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
            'Expires': '0',
        })


@docs(
    tags=['TSDB'],
    summary='Get value ranges from database',
)
@routes.post('/tsdb/ranges')
@request_schema(schemas.TSDBRangesQuerySchema)
async def ranges_endpoint(request: web.Request) -> web.Response:
    return web.json_response(
        await _client(request).ranges(**request['data'])
    )


@docs(
    tags=['TSDB'],
    summary='Get single metrics from database',
)
@routes.post('/tsdb/metrics')
@request_schema(schemas.TSDBMetricsQuerySchema)
async def metrics_endpoint(request: web.Request) -> web.Response:
    return web.json_response(
        await _client(request).metrics(**request['data'])
    )


@docs(
    tags=['TSDB'],
    summary='List available measurements and fields in the database',
)
@routes.post('/tsdb/fields')
@request_schema(schemas.TSDBFieldsQuerySchema)
async def fields_endpoint(request: web.Request) -> web.Response:
    return web.json_response(
        await _client(request).fields(**request['data'])
    )


async def _stream_ranges(app: web.Application, ws: web.WebSocketResponse, id: str, params: dict):
    client = victoria.fget_client(app)
    poll_interval = app['config']['poll_interval']
    open_ended = _check_open_ended(params)
    initial = True

    while True:
        async with protected('ranges query'):
            result = await client.ranges(**params)

            if result:
                await ws.send_json({
                    'id': id,
                    'data': {
                        'initial': initial,
                        'ranges': result,
                    },
                })

                params['start'] = utils.s_time()
                params.pop('duration', None)
                initial = False

        if not open_ended:
            break

        await asyncio.sleep(poll_interval)


async def _stream_metrics(app: web.Application, ws: web.WebSocketResponse, id: str, params: dict):
    client = victoria.fget_client(app)
    poll_interval = app['config']['poll_interval']

    while True:
        async with protected('metrics query'):
            result = await client.metrics(**params)

            await ws.send_json({
                'id': id,
                'data': {
                    'metrics': result,
                },
            })

        await asyncio.sleep(poll_interval)


@docs(
    tags=['TSDB'],
    summary='Open a WebSocket to stream values from database as they are added',
)
@routes.get('/tsdb/stream')
async def stream(request: web.Request) -> web.Response:
    app = request.app
    ws = web.WebSocketResponse()
    streams = {}

    try:
        await ws.prepare(request)
        request.app['websockets'].add(ws)
        cmd_schema = schemas.TSDBStreamCommandSchema()
        ranges_schema = schemas.TSDBRangesQuerySchema()
        metrics_schema = schemas.TSDBMetricsQuerySchema()

        async for msg in ws:
            try:
                msg = json.loads(msg.data)
                schemas.validate(cmd_schema, msg)
                cmd = msg['command']
                id = msg['id']
                query = msg.get('query', {})

                existing: asyncio.Task = streams.pop(id, None)
                existing and existing.cancel()

                if cmd == 'ranges':
                    schemas.validate(ranges_schema, query)
                    streams[id] = asyncio.create_task(_stream_ranges(app, ws, id, query))

                elif cmd == 'metrics':
                    schemas.validate(metrics_schema, query)
                    streams[id] = asyncio.create_task(_stream_metrics(app, ws, id, query))

                elif cmd == 'stop':
                    pass  # We already removed any pre-existing task from streams

                # Marshmallow validates commands
                # This path should never be reached
                else:  # pragma: no cover
                    raise NotImplementedError('Unknown command')

            except asyncio.CancelledError:
                raise

            except Exception as ex:
                LOGGER.error(f'Stream read error {strex(ex)}')
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
