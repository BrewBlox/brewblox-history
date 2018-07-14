"""
Converts REST endpoints into Influx queries
"""

from typing import Callable, Optional

import dpath
from aiohttp import web
from brewblox_service import brewblox_logger

from brewblox_history import influx

LOGGER = brewblox_logger(__name__)
routes = web.RouteTableDef()


DEFAULT_APPROX_POINTS = 100


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
                        approx_points: Optional[int]=None,
                        **_  # allow, but discard all other kwargs
                        ) -> dict:

    query = 'select {keys} from {measurement}'
    keys = ','.join([f'"{key}"' if key is not '*' else key for key in keys])

    query += _find_time_frame(start, duration, end)

    if order_by:
        query += ' order by {order_by}'

    if limit:
        query += ' limit {limit}'

    if approx_points:
        select_params = _prune(locals(), {'measurement', 'database', 'approx_points', 'start', 'duration', 'end'})
        database = await select_downsampling_database(client, **select_params)

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


async def select_downsampling_database(client: influx.QueryClient,
                                       measurement: str,
                                       database: str=influx.DEFAULT_DATABASE,
                                       approx_points: int=DEFAULT_APPROX_POINTS,
                                       start: Optional[str]=None,
                                       duration: Optional[str]=None,
                                       end: Optional[str]=None,
                                       ):
    """
    Chooses the downsampling database that will yield the optimum number of results when queried.
    This is calculated by taking the request number of points (approx_points), and measurement count,
    and dividing the highest value by the lowest.

    The database where this calculation yields the lowest value is chosen.

    Examples, for approx_points = 100:
        [20, 200] => 200
        [90, 200] => 90
        [100, 200] => 100
        [500, 200] => 200
        [20, 600] => 20
    """
    time_frame = _find_time_frame(start, duration, end)

    queries = [f'select count(time) from {{database}}.autogen.{{measurement}}{time_frame}']
    queries += [
        f'select count(time) from {{database}}_{interval}.autogen.{{measurement}}{time_frame}'
        for interval in influx.DOWNSAMPLE_INTERVALS
    ]
    query = ';'.join(queries)

    params = _prune(locals(), {'query', 'database', 'measurement', 'start', 'duration', 'end'})
    query_response = await client.query(**params)

    values = dpath.util.values(query_response, 'results/*/series/0/values/0/0')
    databases = [database] + [f'{database}_{interval}' for interval in influx.DOWNSAMPLE_INTERVALS]

    # Create a dict where key=approximation score, and values=database name
    # Calculate approximation score by comparing it to 'approx_points' target
    # Values are compared by dividing the highest by the lowest
    proximity = {
        (max(approx_points, v) / min(approx_points, v)): db
        for v, db in zip(values, databases)
    }

    # Lowest score indicates closest to target value
    return proximity[min(*proximity.keys())]


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
                order_by:
                    type: string
                    required: false
                    example: "time asc"
                approx_points:
                    type: int
                    required: false
                    example: 100
    """
    return await _do_with_handler(select_values, request)
