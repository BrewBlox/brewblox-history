"""
REST endpoints for TimeSeries queries
"""

import asyncio
import json
from contextlib import asynccontextmanager

from aiohttp import web
from aiohttp_apispec import docs, request_schema
from brewblox_service import brewblox_logger, strex

from brewblox_history import schemas, utils, victoria

LOGGER = brewblox_logger(__name__, True)
routes = web.RouteTableDef()


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
    tags=['TimeSeries'],
    summary='Ping the database',
)
@routes.get('/timeseries/ping')
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
    tags=['TimeSeries'],
    summary='Get value ranges from database',
)
@routes.post('/timeseries/ranges')
@request_schema(schemas.TimeSeriesRangesQuerySchema)
async def ranges_endpoint(request: web.Request) -> web.Response:
    return web.json_response(
        await _client(request).ranges(**request['data'])
    )


@docs(
    tags=['TimeSeries'],
    summary='Get single metrics from database',
)
@routes.post('/timeseries/metrics')
@request_schema(schemas.TimeSeriesMetricsQuerySchema)
async def metrics_endpoint(request: web.Request) -> web.Response:
    return web.json_response(
        await _client(request).metrics(**request['data'])
    )


@docs(
    tags=['TimeSeries'],
    summary='List available measurements and fields in the database',
)
@routes.post('/timeseries/fields')
@request_schema(schemas.TimeSeriesFieldsQuerySchema)
async def fields_endpoint(request: web.Request) -> web.Response:
    return web.json_response(
        await _client(request).fields(**request['data'])
    )


async def _stream_ranges(app: web.Application, ws: web.WebSocketResponse, id: str, params: dict):
    client = victoria.fget_client(app)
    interval = app['config']['ranges_interval']
    open_ended = utils.is_open_ended(**params)
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

        await asyncio.sleep(interval)


async def _stream_metrics(app: web.Application, ws: web.WebSocketResponse, id: str, params: dict):
    writer = victoria.fget_writer(app)
    cache = {}
    interval = app['config']['metrics_interval']

    def cb(point):
        field = point['metric']
        if field in params['fields']:
            cache[field] = point

    try:
        writer.listeners.add(cb)

        while True:
            async with protected('metrics push'):
                await asyncio.sleep(interval)
                await ws.send_json({
                    'id': id,
                    'data': {
                        'metrics': list(cache.values()),
                    },
                })

    finally:
        writer.listeners.remove(cb)


@docs(
    tags=['TimeSeries'],
    summary='Open a WebSocket to stream values from database as they are added',
)
@routes.get('/timeseries/stream')
async def stream(request: web.Request) -> web.Response:
    app = request.app
    ws = web.WebSocketResponse()
    streams = {}

    try:
        await ws.prepare(request)
        request.app['websockets'].add(ws)
        cmd_schema = schemas.TimeSeriesStreamCommandSchema()
        ranges_schema = schemas.TimeSeriesRangesQuerySchema()
        metrics_schema = schemas.TimeSeriesMetricsQuerySchema()

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
