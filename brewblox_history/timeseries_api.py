"""
REST endpoints for TimeSeries queries
"""

import asyncio
import json
from contextlib import asynccontextmanager

from aiohttp import web
from aiohttp_apispec import docs, json_schema, response_schema
from brewblox_service import brewblox_logger, strex

from brewblox_history import schemas, socket_closer, utils, victoria

LOGGER = brewblox_logger(__name__, True)
routes = web.RouteTableDef()


def _client(request: web.Request) -> victoria.VictoriaClient:
    return victoria.fget(request.app)


@asynccontextmanager
async def protected(desc: str):
    try:
        yield
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
    summary='List available measurements and fields in the database',
)
@routes.post('/timeseries/fields')
@json_schema(schemas.TimeSeriesFieldsQuerySchema)
async def fields_endpoint(request: web.Request) -> web.Response:
    return web.json_response(
        await _client(request).fields(**request['json'])
    )


@docs(
    tags=['TimeSeries'],
    summary='Get value ranges from database',
)
@routes.post('/timeseries/ranges')
@json_schema(schemas.TimeSeriesRangesQuerySchema)
@response_schema(schemas.TimeSeriesRangeResponseSchema(many=True))
async def ranges_endpoint(request: web.Request) -> web.Response:
    return web.json_response(
        await _client(request).ranges(**request['json'])
    )


@docs(
    tags=['TimeSeries'],
    summary='Get single metrics from database',
)
@routes.post('/timeseries/metrics')
@json_schema(schemas.TimeSeriesMetricsQuerySchema)
@response_schema(schemas.TimeSeriesMetricResponseSchema(many=True))
async def metrics_endpoint(request: web.Request) -> web.Response:
    return web.json_response(
        await _client(request).metrics(**request['json'])
    )


@docs(
    tags=['TimeSeries'],
    summary='Get value ranges from database',
)
@routes.post('/timeseries/csv')
@json_schema(schemas.TimeSeriesCsvQuerySchema)
async def csv_endpoint(request: web.Request) -> web.Response:
    response = web.StreamResponse(
        status=200,
        reason='OK',
        headers={
            'Content-Type': 'text/plain',
            'Access-Control-Allow-Origin': '*',
        }
    )
    await response.prepare(request)

    async for line in _client(request).csv(**request['json']):  # pragma: no branch
        await response.write(line.encode())
        await response.write('\n'.encode())

    await response.write_eof()
    return response


async def _stream_ranges(app: web.Application, ws: web.WebSocketResponse, id: str, params: dict):
    client = victoria.fget(app)
    interval = app['config']['ranges_interval']
    open_ended = utils.is_open_ended(**params)
    initial = True

    while True:
        async with protected('ranges query'):
            await ws.send_json({
                'id': id,
                'data': {
                    'initial': initial,
                    'ranges': await client.ranges(**params),
                },
            })

            params['start'] = utils.now()
            params.pop('duration', None)
            initial = False

        if not open_ended:
            break

        await asyncio.sleep(interval)


async def _stream_metrics(app: web.Application, ws: web.WebSocketResponse, id: str, params: dict):
    client = victoria.fget(app)
    interval = app['config']['metrics_interval']
    fields = params['fields']

    while True:
        async with protected('metrics push'):
            await ws.send_json({
                'id': id,
                'data': {
                    'metrics': await client.metrics(fields),
                },
            })

        await asyncio.sleep(interval)


@docs(
    tags=['TimeSeries'],
    summary='Open a WebSocket to stream values from database as they are added',
    responses={101: {'description': 'WebSocket upgrade'}}
)
@routes.get('/timeseries/stream')
async def stream(request: web.Request) -> web.Response:
    app = request.app
    ws = web.WebSocketResponse()
    streams = {}

    try:
        await ws.prepare(request)
        socket_closer.add(request.app, ws)
        cmd_schema = schemas.TimeSeriesStreamCommandSchema()
        ranges_schema = schemas.TimeSeriesRangesQuerySchema()
        metrics_schema = schemas.TimeSeriesMetricsQuerySchema()

        async for msg in ws:  # pragma: no branch
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

            except Exception as ex:
                LOGGER.error(f'Stream read error {strex(ex)}')
                await ws.send_json({
                    'error': strex(ex),
                    'message': msg,
                })

    finally:
        socket_closer.discard(app, ws)
        # Coverage complains about next line -> exit not being covered
        for task in streams.values():  # pragma: no cover
            task.cancel()

    return ws


def setup(app: web.Application):
    app.router.add_routes(routes)
