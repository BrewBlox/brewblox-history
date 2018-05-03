"""
Converts REST endpoints into Influx queries
"""


import logging

import dpath
from aiohttp import web
from brewblox_history import influx

LOGGER = logging.getLogger(__name__)
routes = web.RouteTableDef()


def setup(app: web.Application):
    app.router.add_routes(routes)
    app.middlewares.append(controller_error_middleware)


@web.middleware
async def controller_error_middleware(request: web.Request, handler: web.RequestHandler) -> web.Response:
    try:
        return await handler(request)
    except Exception as ex:
        LOGGER.warn(f'REST error: {ex}')
        return web.json_response({'error': str(ex)}, status=500)


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
    args = await request.json()
    query_str = args['query']
    database = args.get('database', None)

    data = await influx.get_client(request.app).query(
        query=query_str,
        database=database)
    return web.json_response(data)


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
    args = await request.json()

    client = influx.get_client(request.app)
    database = args.get('database', influx.DEFAULT_DATABASE)
    query = 'show field keys'
    params = dict()

    if 'measurement' in args:
        query += ' from {measurement}'
        params['measurement'] = args['measurement']

    query_response = await client.query(
        query=query,
        database=database,
        **params
    )

    response = dict()

    for path, meas_name in dpath.util.search(
            query_response, 'results/*/series/*/name', yielded=True, dirs=False):

        # results/[index]/series/[index]/values/*/0
        values_glob = '/'.join(path.split('/')[:-1] + ['values', '*', '0'])
        response[meas_name] = dpath.util.values(query_response, values_glob)

    return web.json_response(response)


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
                keys:
                    type: list
                    example: ["*"]
                duration:
                    type: string
                    required: false
                    example: "10m"
                end:
                    type: string
                    example: 
                limit:
                    type: int
                    default: 100
                    example: 100
    """
    args = await request.json()

    client = influx.get_client(request.app)
    database = args.get('database', influx.DEFAULT_DATABASE)
    query = 'select {keys}'
    params = dict(keys=','.join(args['keys']))

    if 'measurement' in args:
        query += ' from {measurement}'
        params['measurement'] = args['measurement']

    if 'duration' in args:
        query += ' where time > now() - {duration}'
        params['duration'] = args['duration']

    if 'limit' in args:
        query += ' limit {limit}'
        params['limit'] = args['limit']

    LOGGER.info(query)
    LOGGER.info(params)

    query_response = await client.query(
        query=query,
        database=database,
        **params
    )

    return web.json_response(query_response)
