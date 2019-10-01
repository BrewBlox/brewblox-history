"""
REST endpoints for queries
"""

import asyncio
from typing import Callable

from aiohttp import web
from brewblox_service import brewblox_logger, strex

from brewblox_history import influx
from brewblox_history.queries import (configure_db, raw_query,
                                      select_last_values, select_values,
                                      show_keys)

LOGGER = brewblox_logger(__name__)
routes = web.RouteTableDef()


def setup(app: web.Application):
    app.router.add_routes(routes)
    app.middlewares.append(controller_error_middleware)


@web.middleware
async def controller_error_middleware(request: web.Request, handler: web.RequestHandler) -> web.Response:
    try:
        return await handler(request)
    except asyncio.CancelledError:  # pragma: no cover
        raise
    except Exception as ex:
        LOGGER.error(f'REST error: {strex(ex)}', exc_info=request.app['config']['debug'])
        return web.json_response({'error': strex(ex)}, status=500)


async def _do_with_handler(func: Callable, request: web.Request) -> web.Response:
    args = await request.json()
    response = await func(influx.get_client(request.app), **args)
    return web.json_response(response)


@routes.post('/_debug/query')
async def custom_query(request: web.Request) -> web.Response:
    """
    ---
    tags:
    - History
    summary: Query InfluxDB
    description: Send a string query to the database.
    operationId: history.query
    produces:
    - application/json
    parameters:
    -
        in: body
        name: body
        description: Query
        required: true
        schema:
            type: object
            properties:
                database:
                    type: string
                query:
                    type: string
    """
    return await _do_with_handler(raw_query, request)


@routes.get('/ping')
async def ping(request: web.Request) -> web.Response:
    """
    ---
    tags:
    - History
    summary: Ping InfluxDB
    operationId: history.ping
    produces:
    - application/json
    """
    await influx.get_client(request.app).ping()
    return web.json_response({'ok': True})


@routes.post('/query/objects')
async def objects_query(request: web.Request) -> web.Response:
    """
    ---
    tags:
    - History
    summary: List objects
    description: List available measurements and objects in database.
    operationId: history.query.objects
    produces:
    - application/json
    parameters:
    -
        in: body
        name: body
        required: true
        schema:
            type: object
            properties:
                database:
                    type: string
                    required: false
                measurement:
                    type: string
                    required: false
    """
    return await _do_with_handler(show_keys, request)


@routes.post('/query/values')
async def values_query(request: web.Request) -> web.Response:
    """
    ---
    tags:
    - History
    summary: Get object values
    operationId: history.query.values
    produces:
    - application/json
    parameters:
    -
        in: body
        name: body
        required: true
        schema:
            type: object
            properties:
                database:
                    type: string
                    required: false
                measurement:
                    type: string
                    required: true
                fields:
                    type: list
                    required: true
                    example: ["*"]
                start:
                    type: string
                    required: false
                    example: "1439873640000000000"
                duration:
                    type: string
                    required: false
                    example: "10m"
                end:
                    type: string
                    required: false
                    example: "1439873640000000000"
                limit:
                    type: int
                    required: false
                    example: 100
                order_by:
                    type: string
                    required: false
                    example: "time asc"
                policy:
                    type: string
                    required: false
                    example: "downsample_1m"
                approx_points:
                    type: int
                    required: false
                    example: 100
    """
    return await _do_with_handler(select_values, request)


@routes.post('/query/last_values')
async def last_values_query(request: web.Request) -> web.Response:
    """
    ---
    tags:
    - History
    summary: Get current object values
    operationId: history.query.last_values
    produces:
    - application/json
    parameters:
    -
        in: body
        name: body
        required: true
        schema:
            type: object
            properties:
                database:
                    type: string
                    required: false
                measurement:
                    type: string
                    required: true
                fields:
                    type: list
                    required: true
                    example: ["actuator-1/value"]
                duration:
                    type: string
                    required: false
                    example: "10m"
    """
    return await _do_with_handler(select_last_values, request)


@routes.post('/query/configure')
async def configure_db_query(request: web.Request) -> web.Response:
    """
    ---
    tags:
    - History
    summary: Configure database
    operationId: history.query.configure
    produces:
    - application/json
    """
    return web.json_response(
        await configure_db(influx.get_client(request.app)))
