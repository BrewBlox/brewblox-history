"""
REST endpoints for datastore queries
"""

from fastapi import APIRouter, Response

from . import redis
from .models import (DatastoreDeleteResponse, DatastoreMultiQuery,
                     DatastoreMultiValueBox, DatastoreOptSingleValueBox,
                     DatastoreSingleQuery, DatastoreSingleValueBox)
from .settings import brewblox_logger

LOGGER = brewblox_logger(__name__)

router = APIRouter(prefix='/datastore', tags=['Datastore'])


@router.get('/ping')
async def ping(response: Response):
    """
    Ping datastore, checking availability.
    """
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate, proxy-revalidate, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    await redis.client.get().ping()
    return {'ping': 'pong'}


@router.post('/get')
async def datastore_get(args: DatastoreSingleQuery) -> DatastoreOptSingleValueBox:
    """
    Get a specific object from the datastore.
    """
    value = await redis.client.get().get(args.namespace, args.id)
    return DatastoreOptSingleValueBox(value=value)


@router.post('/mget')
async def datastore_mget(args: DatastoreMultiQuery) -> DatastoreMultiQuery:
    """
    Get multiple objects from the datastore.
    """
    values = await redis.client.get().mget(args.namespace, args.ids, args.filter)
    return DatastoreMultiValueBox(values=values)


@router.post('/set')
async def datastore_set(args: DatastoreOptSingleValueBox) -> DatastoreSingleValueBox:
    """
    Create or update an object in the datastore.
    """
    value = await redis.client.get().set(args.value)
    return DatastoreSingleValueBox(value=value)


@router.post('/mset')
async def datastore_mset(args: DatastoreMultiValueBox) -> DatastoreMultiValueBox:
    """
    Create or update multiple objects in the datastore.
    """
    values = await redis.client.get().mset(args.values)
    return DatastoreMultiValueBox(values=values)


@router.post('/delete')
async def datastore_delete(args: DatastoreSingleQuery) -> DatastoreDeleteResponse:
    """
    Remove a single object from the datastore.
    """
    count = await redis.client.get().delete(args.namespace, args.id)
    return DatastoreDeleteResponse(count=count)


@router.post('/mdelete')
async def datastore_mdelete(args: DatastoreMultiQuery) -> DatastoreDeleteResponse:
    """
    Remove multiple objects from the datastore.
    """
    count = await redis.client.get().mdelete(args.namespace, args.ids, args.filter)
    return DatastoreDeleteResponse(count=count)
