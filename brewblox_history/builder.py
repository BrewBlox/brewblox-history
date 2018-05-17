"""
Converts REST endpoints into Influx queries
"""

from typing import Optional, Callable

import dpath
from aiohttp import web
from brewblox_history import influx
from brewblox_service import brewblox_logger

LOGGER = brewblox_logger(__name__)
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
        return web.json_response({'error': f'{type(ex).__name__}={ex}'}, status=500)


########################################################################################################


async def _do_with_handler(func: Callable, request: web.Request) -> web.Response:
    args = await request.json()
    response = await func(influx.get_client(request.app), **args)
    return web.json_response(response)


def _prune(vals: dict, relevant: set) -> dict:
    """Creates a dict only containing meaningful and relevant key/value pairs.

    All pairs in the returned dict met three conditions:
    * Their key was in `relevant`
    * Their key was in `vals`
    * The value associated with the key in `vals` was not None
    """
    return {k: vals[k] for k in relevant if vals.get(k) is not None}


def _find_time_frame(start: Optional[str], duration: Optional[str], end: Optional[str]) -> str:
    """
    Determines required InfluxDB QL where/and clause required to express time frame.

    A time frame can be constructed from a start, a duration, and an end.
    At most two determinators can be present simultaneously.
    """
    clause = ''

    if all([start, duration, end]):
        raise ValueError('At most two out of three duration arguments can be provided')

    elif not any([start, duration, end]):
        pass

    elif start and duration:
        clause = ' where time >= {start} and time <= {start} + {duration}'

    elif start and end:
        clause = ' where time >= {start} and time <= {end}'

    elif duration and end:
        clause = ' where time >= {end} - {duration} and time <= {end}'

    elif start:
        clause = ' where time >= {start}'

    elif duration:
        clause = ' where time >= now() - {duration}'

    elif end:
        clause = ' where time <= {end}'

    else:  # pragma: no cover
        # This path should never be reached
        raise RuntimeError('Unexpected code path while determining time frame!')

    return clause


########################################################################################################


async def raw_query(client: influx.QueryClient,
                    database: Optional[str]=influx.DEFAULT_DATABASE,
                    query: Optional[str]='show databases'
                    ) -> dict:

    return await client.query(
        query=query,
        database=database
    )


async def show_keys(client: influx.QueryClient,
                    database: Optional[str]=None,
                    measurement: Optional[str]=None,
                    **_  # allow, but discard all other kwargs
                    ) -> dict:

    query = 'show field keys'

    if measurement:
        query += ' from {measurement}'

    params = _prune(locals(), {'query', 'database', 'measurement'})
    query_response = await client.query(**params)

    response = dict()

    for path, meas_name in dpath.util.search(
            query_response, 'results/*/series/*/name', yielded=True, dirs=False):

        # results/[index]/series/[index]/values/*/0
        values_glob = '/'.join(path.split('/')[:-1] + ['values', '*', '0'])
        response[meas_name] = dpath.util.values(query_response, values_glob)

    return response


async def select_values(client: influx.QueryClient,
                        measurement: str,
                        keys: Optional[list]=['*'],
                        database: Optional[str]=None,
                        start: Optional[str]=None,
                        duration: Optional[str]=None,
                        end: Optional[str]=None,
                        order_by: Optional[str]=None,
                        limit: Optional[int]=None,
                        **_  # allow, but discard all other kwargs
                        ) -> dict:

    query = 'select {keys} from {measurement}'
    keys = ','.join([f'"{key}"' if key is not '*' else key for key in keys])

    query += _find_time_frame(start, duration, end)

    if order_by:
        query += ' order by {order_by}'

    if limit:
        query += ' limit {limit}'

    params = _prune(locals(), {'query', 'database', 'measurement', 'keys',
                               'start', 'duration', 'end', 'order_by', 'limit'})
    query_response = await client.query(**params)

    try:
        # Only support single-measurement queries
        response = dpath.util.get(query_response, 'results/0/series/0')
    except KeyError:
        # Nothing found
        response = dict()

    return response


########################################################################################################


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
                keys:
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
    """
    return await _do_with_handler(select_values, request)
