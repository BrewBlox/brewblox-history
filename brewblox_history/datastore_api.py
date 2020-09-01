"""
REST endpoints for datastore queries
"""

from aiohttp import web
from aiohttp_apispec import docs, request_schema, response_schema
from brewblox_service import brewblox_logger

from brewblox_history import redis, schemas

LOGGER = brewblox_logger(__name__)
routes = web.RouteTableDef()


def setup(app: web.Application):
    app.router.add_routes(routes)


@docs(
    tags=['Datastore'],
    summary='Ping datastore, checking availability',
)
@routes.get('/datastore/ping')
async def ping(request: web.Request) -> web.Response:
    return web.json_response(
        await redis.get_redis(request.app).ping()
    )


@docs(
    tags=['Datastore'],
    summary='Get single object from database / namespace',
)
@routes.post('/datastore/get')
@request_schema(schemas.DatastoreKeyQuerySchema)
@response_schema(schemas.DatastoreValueSchema)
async def get(request: web.Request) -> web.Response:
    return web.json_response(
        await redis.get_redis(request.app).get(**request['data'])
    )


@docs(
    tags=['Datastore'],
    summary='Get multiple objects from database / namespace',
)
@routes.post('/datastore/mget')
@request_schema(schemas.DatastoreKeyFilterQuerySchema)
@response_schema(schemas.DatastoreValueSchema(many=True))
async def mget(request: web.Request) -> web.Response:
    return web.json_response(
        await redis.get_redis(request.app).mget(**request['data'])
    )


@docs(
    tags=['Datastore'],
    summary='Write object to datastore',
)
@routes.post('/datastore/set')
@request_schema(schemas.DatastoreValueQuerySchema)
@response_schema(schemas.DatastoreValueSchema)
async def set(request: web.Request) -> web.Response:
    return web.json_response(
        await redis.get_redis(request.app).set(**request['data'])
    )


@docs(
    tags=['Datastore'],
    summary='Write multiple objects to datastore',
)
@routes.post('/datastore/mset')
@request_schema(schemas.DatastoreValueListQuerySchema)
@response_schema(schemas.DatastoreValueSchema(many=True))
async def mset(request: web.Request) -> web.Response:
    return web.json_response(
        await redis.get_redis(request.app).mset(**request['data'])
    )


@docs(
    tags=['Datastore'],
    summary='Remove object from datastore',
)
@routes.post('/datastore/delete')
@request_schema(schemas.DatastoreKeyQuerySchema)
async def delete(request: web.Request) -> web.Response:
    return web.json_response(
        await redis.get_redis(request.app).delete(**request['data'])
    )


@docs(
    tags=['Datastore'],
    summary='Remove object from datastore',
)
@routes.post('/datastore/mdelete')
@request_schema(schemas.DatastoreKeyFilterQuerySchema)
async def mdelete(request: web.Request) -> web.Response:
    return web.json_response(
        await redis.get_redis(request.app).mdelete(**request['data'])
    )
