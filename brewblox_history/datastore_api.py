"""
REST endpoints for datastore queries
"""

from aiohttp import web
from aiohttp_pydantic import PydanticView
from aiohttp_pydantic.oas.typing import r200
from brewblox_service import brewblox_logger

from brewblox_history import redis
from brewblox_history.models import (DatastoreDeleteResponse,
                                     DatastoreMultiQuery,
                                     DatastoreMultiValueBox,
                                     DatastoreOptSingleValueBox,
                                     DatastoreSingleQuery,
                                     DatastoreSingleValueBox)

LOGGER = brewblox_logger(__name__)
routes = web.RouteTableDef()


class RedisView(PydanticView):
    def __init__(self, request: web.Request) -> None:
        super().__init__(request)
        self.redis = redis.fget(request.app)


@routes.view('/datastore/ping')
class PingView(RedisView):
    async def get(self) -> r200[dict]:
        """
        Ping datastore, checking availability.

        Tags: Datastore
        """
        await self.redis.ping()
        return web.json_response(
            data={'ping': 'pong'},
            headers={
                'Cache-Control': 'no-cache',
                'Pragma': 'no-cache',
                'Expires': '0',
            })


@routes.view('/datastore/get')
class GetView(RedisView):
    async def post(self, args: DatastoreSingleQuery) -> r200[DatastoreOptSingleValueBox]:
        """
        Get specific object from the datastore.

        Tags: Datastore
        """
        value = await self.redis.get(args.namespace, args.id)
        return web.json_response(
            DatastoreOptSingleValueBox(value=value).dict()
        )


@routes.view('/datastore/mget')
class MGetView(RedisView):
    async def post(self, args: DatastoreMultiQuery) -> r200[DatastoreMultiValueBox]:
        """
        Get multiple objects from the datastore.

        Tags: Datastore
        """
        values = await self.redis.mget(args.namespace, args.ids, args.filter)
        return web.json_response(
            DatastoreMultiValueBox(values=values).dict()
        )


@routes.view('/datastore/set')
class SetView(RedisView):
    async def post(self, args: DatastoreSingleValueBox) -> r200[DatastoreSingleValueBox]:
        """
        Create or update an object in the datastore.

        Tags: Datastore
        """
        value = await self.redis.set(args.value)
        return web.json_response(
            DatastoreSingleValueBox(value=value).dict()
        )


@routes.view('/datastore/mset')
class MSetView(RedisView):
    async def post(self, args: DatastoreMultiValueBox) -> r200[DatastoreMultiValueBox]:
        """
        Create or update multiple objects in the datastore.

        Tags: Datastore
        """
        values = await self.redis.mset(args.values)
        return web.json_response(
            DatastoreMultiValueBox(values=values).dict()
        )


@routes.view('/datastore/delete')
class DeleteView(RedisView):
    async def post(self, args: DatastoreSingleQuery) -> r200[DatastoreDeleteResponse]:
        """
        Remove a single object from the datastore.

        Tags: Datastore
        """
        count = await self.redis.delete(args.namespace, args.id)
        return web.json_response(
            DatastoreDeleteResponse(count=count).dict()
        )


@routes.view('/datastore/mdelete')
class MDeleteView(RedisView):
    async def post(self, args: DatastoreMultiQuery) -> r200[DatastoreDeleteResponse]:
        """
        Remove multiple objects from the datastore.

        Tags: Datastore
        """
        count = await self.redis.mdelete(args.namespace, args.ids, args.filter)
        return web.json_response(
            DatastoreDeleteResponse(count=count).dict()
        )


def setup(app: web.Application):
    app.router.add_routes(routes)
