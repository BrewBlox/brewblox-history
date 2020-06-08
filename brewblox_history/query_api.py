"""
REST endpoints for queries
"""

from aiohttp import web
from aiohttp_apispec import docs, request_schema
from brewblox_service import brewblox_logger, strex

from brewblox_history import influx, schemas
from brewblox_history.queries import (configure_db, raw_query,
                                      select_last_values, select_values,
                                      show_keys)

LOGGER = brewblox_logger(__name__)
routes = web.RouteTableDef()


def setup(app: web.Application):
    app.router.add_routes(routes)
    app.middlewares.append(controller_error_middleware)


def _client(request: web.Request) -> influx.QueryClient:
    return influx.get_client(request.app)


@web.middleware
async def controller_error_middleware(request: web.Request, handler: web.RequestHandler) -> web.Response:
    try:
        return await handler(request)
    except Exception as ex:
        LOGGER.error(f'REST error: {strex(ex)}', exc_info=request.app['config']['debug'])
        return web.json_response({'error': strex(ex)}, status=500)


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
    print(request['data'])
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
    summary='Configure database',
)
@routes.post('/query/configure')
async def configure_db_query(request: web.Request) -> web.Response:
    return web.json_response(
        await configure_db(_client(request), verbose=False)
    )
