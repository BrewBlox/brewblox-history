"""
REST endpoints for datastore queries
"""

from aiohttp import web
from aiohttp_apispec import docs, json_schema, response_schema
from brewblox_service import brewblox_logger

from brewblox_history import redis, schemas

LOGGER = brewblox_logger(__name__)
routes = web.RouteTableDef()


@docs(
    tags=['Datastore'],
    summary='Ping datastore, checking availability',
)
@routes.get('/datastore/ping')
async def ping(request: web.Request) -> web.Response:
    return web.json_response(
        data={'ping': await redis.fget(request.app).ping()},
        headers={
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
            'Expires': '0',
        })


@docs(
    tags=['Datastore'],
    summary='Get single object from database / namespace',
)
@routes.post('/datastore/get')
@json_schema(schemas.DatastoreSingleQuerySchema)
@response_schema(schemas.DatastoreSingleValueSchema)
async def get(request: web.Request) -> web.Response:
    return web.json_response({
        'value': await redis.fget(request.app).get(**request['json'])
    })


@docs(
    tags=['Datastore'],
    summary='Get multiple objects from database / namespace',
)
@routes.post('/datastore/mget')
@json_schema(schemas.DatastoreMultiQuerySchema)
@response_schema(schemas.DatastoreMultiValueSchema)
async def mget(request: web.Request) -> web.Response:
    return web.json_response({
        'values': await redis.fget(request.app).mget(**request['json'])
    })


@docs(
    tags=['Datastore'],
    summary='Write object to datastore',
)
@routes.post('/datastore/set')
@json_schema(schemas.DatastoreSingleValueSchema)
@response_schema(schemas.DatastoreSingleValueSchema)
async def set(request: web.Request) -> web.Response:
    return web.json_response({
        'value': await redis.fget(request.app).set(**request['json'])
    })


@docs(
    tags=['Datastore'],
    summary='Write multiple objects to datastore',
)
@routes.post('/datastore/mset')
@json_schema(schemas.DatastoreMultiValueSchema)
@response_schema(schemas.DatastoreMultiValueSchema)
async def mset(request: web.Request) -> web.Response:
    return web.json_response({
        'values': await redis.fget(request.app).mset(**request['json'])
    })


@docs(
    tags=['Datastore'],
    summary='Remove object from datastore',
)
@routes.post('/datastore/delete')
@json_schema(schemas.DatastoreSingleQuerySchema)
@response_schema(schemas.DatastoreDeleteResponseSchema)
async def delete(request: web.Request) -> web.Response:
    return web.json_response({
        'count': await redis.fget(request.app).delete(**request['json'])
    })


@docs(
    tags=['Datastore'],
    summary='Remove multiple objects from datastore',
)
@routes.post('/datastore/mdelete')
@json_schema(schemas.DatastoreMultiQuerySchema)
@response_schema(schemas.DatastoreDeleteResponseSchema)
async def mdelete(request: web.Request) -> web.Response:
    return web.json_response({
        'count': await redis.fget(request.app).mdelete(**request['json'])
    })


def setup(app: web.Application):
    app.router.add_routes(routes)
