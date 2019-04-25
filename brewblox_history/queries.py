"""
Converts REST endpoints into Influx queries
"""

import asyncio
import re
import time
from contextlib import suppress
from typing import Awaitable, Callable, List, Optional, Tuple

import dpath
from aiohttp import web
from aioinflux import InfluxDBError
from brewblox_service import brewblox_logger, strex
from dateutil import parser as date_parser

from brewblox_history import influx

LOGGER = brewblox_logger(__name__)
routes = web.RouteTableDef()


DEFAULT_APPROX_POINTS = 200


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
    """Determines required InfluxDB QL where/and clause required to express time frame.

    A time frame can be constructed from a start, a duration, and an end.
    At most two determinators can be present simultaneously.
    """
    clause = ''

    if all([start, duration, end]):
        raise ValueError('At most two out of three duration arguments can be provided')

    elif not any([start, duration, end]):
        pass

    elif start and duration:
        clause = ' WHERE time >= {start} AND time <= {start} + {duration}'

    elif start and end:
        clause = ' WHERE time >= {start} AND time <= {end}'

    elif duration and end:
        clause = ' WHERE time >= {end} - {duration} AND time <= {end}'

    elif start:
        clause = ' WHERE time >= {start}'

    elif duration:
        clause = ' WHERE time >= now() - {duration}'

    elif end:
        clause = ' WHERE time <= {end}'

    else:  # pragma: no cover
        # This path should never be reached
        raise RuntimeError('Unexpected code path while determining time frame!')

    return clause


########################################################################################################


def format_fields(keys: List[str], prefix: str = '') -> str:
    """Formats key list as a single comma-separated and quoted string

    Each successive downsample adds a prefix to inserted values.
    We must also add this prefix when selecting fields.
    """
    return ','.join([f'"{prefix}{key}"' if key != '*' else key for key in keys])


async def configure_params(client: influx.QueryClient,
                           measurement: str,
                           fields: Optional[List[str]] = ['*'],
                           database: Optional[str] = influx.DEFAULT_DATABASE,
                           start: Optional[str] = None,
                           duration: Optional[str] = None,
                           end: Optional[str] = None,
                           order_by: Optional[str] = None,
                           limit: Optional[int] = None,
                           approx_points: Optional[int] = DEFAULT_APPROX_POINTS,
                           **_  # allow, but discard all other kwargs
                           ) -> dict:
    def nanosecond_date(dt):
        if isinstance(dt, str):
            dt = date_parser.parse(dt)
            return int(dt.timestamp() - time.timezone) * 10 ** 9 + dt.microsecond * 1000
        return dt

    start = nanosecond_date(start)
    duration = duration if not duration else duration.replace(' ', '')
    end = nanosecond_date(end)

    approx_points = int(approx_points)
    select_params = _prune(locals(), {'measurement', 'database',
                                      'approx_points', 'start', 'duration', 'end'})
    policy, prefix = await select_downsampling_policy(client, **select_params)

    # Workaround for https://github.com/influxdata/influxdb/issues/7332
    # The continuous query that fills the downsampled database inserts "key" as "m_key"
    fields = format_fields(fields, prefix)

    return _prune(locals(), {'query', 'database', 'policy', 'measurement', 'fields',
                             'start', 'duration', 'end', 'order_by', 'limit', 'prefix'})


def build_query(params: dict):
    query = 'SELECT {fields} FROM "{database}"."{policy}"."{measurement}"'

    query += _find_time_frame(
        params.get('start'),
        params.get('duration'),
        params.get('end'),
    )

    if 'order_by' in params:
        query += ' ORDER BY {order_by}'

    if 'limit' in params:
        query += ' LIMIT {limit}'

    return query


async def run_query(client: influx.QueryClient, query: str, params: dict):
    query_response = await client.query(query=query, **params)

    try:
        # Only support single-measurement queries
        response = dpath.util.get(query_response, 'results/0/series/0')
        # Workaround for https://github.com/influxdata/influxdb/issues/7332
        # The continuous query that fills the downsampled database inserts "key" as "m_key"
        prefix = params.get('prefix')
        if prefix:
            response['columns'] = [re.sub(prefix, '', v, count=1) for v in response.get('columns', [])]
    except KeyError:
        # Nothing found
        response = dict()

    response['database'] = params.get('database') or influx.DEFAULT_DATABASE
    response['policy'] = params.get('policy') or influx.DEFAULT_POLICY
    return response


async def select_downsampling_policy(client: influx.QueryClient,
                                     measurement: str,
                                     database: str,
                                     approx_points: Optional[int] = None,
                                     start: Optional[str] = None,
                                     duration: Optional[str] = None,
                                     end: Optional[str] = None,
                                     ) -> Awaitable[Tuple[str, str]]:
    """
    Chooses the downsampling policy that will yield the optimum number of results when queried.
    This is calculated by taking the request number of points (approx_points), and measurement count,
    and dividing the highest value by the lowest.

    The policy where this calculation yields the lowest value is chosen.

    Examples, for approx_points = 100:
        [20, 200] => 200
        [90, 200] => 90
        [100, 200] => 100
        [500, 200] => 200
        [20, 600] => 20

    return name of the policy, and the prefix that should be added/stripped from fields in said policy
    """
    default_result = (influx.DEFAULT_POLICY, '')

    if not approx_points:
        return default_result

    time_frame = _find_time_frame(start, duration, end)
    all_policies = dpath.util.values(
        await client.query(f'SHOW RETENTION POLICIES ON "{database}"'),
        'results/0/series/0/values/*/0')

    queries = [
        f'SELECT count(/(m_)*{influx.COMBINED_POINTS_FIELD}/) FROM "{database}"."{policy}"."{measurement}"{time_frame}'
        for policy in all_policies
    ]
    query = ';'.join(queries)

    params = _prune(locals(), {'start', 'duration', 'end'})
    query_response = await client.query(query, **params)

    best_result = default_result
    best_count = None
    best_score = None

    for policy, result in zip(all_policies, query_response['results']):
        try:
            series = result['series'][0]
        except KeyError:
            continue  # No values in range

        field_name = series['columns'][1]  # time is at 0
        count = series['values'][0][1]  # time is at 0
        prefix = field_name[len('count_'):field_name.find(influx.COMBINED_POINTS_FIELD)]

        # Calculate approximation score by comparing actual point count to 'approx_points'
        # Values are compared by dividing the highest by the lowest
        # Lower scores are better
        score = max(approx_points, count) / min(approx_points, count)

        if best_score is None or score < best_score:
            best_result = (policy, prefix)
            best_count = count
            best_score = score

    LOGGER.info(f'Selected {database}.{best_result[0]}.{measurement}, target={approx_points}, actual={best_count}')
    return best_result


########################################################################################################


async def raw_query(client: influx.QueryClient,
                    database: Optional[str] = influx.DEFAULT_DATABASE,
                    query: Optional[str] = 'show databases'
                    ) -> dict:
    """Runs an arbitrary user-defined query

    Note: this is supported only for debugging reasons.
    Production code should never assume this function/endpoint is available.
    """
    return await client.query(query=query, database=database)


async def show_keys(client: influx.QueryClient,
                    database: Optional[str] = None,
                    measurement: Optional[str] = None,
                    **_  # allow, but discard all other kwargs
                    ) -> dict:
    """Selects available keys (without data) from Influx."""
    query = 'SHOW FIELD KEYS'

    if measurement:
        query += ' FROM "{measurement}"'

    params = _prune(locals(), {'query', 'database', 'measurement'})
    query_response = await client.query(**params)

    response = dict()

    for path, meas_name in dpath.util.search(
            query_response, 'results/*/series/*/name', yielded=True, dirs=False):

        # results/[index]/series/[index]/values/*/0
        values_glob = '/'.join(path.split('/')[:-1] + ['values', '*', '0'])
        response[meas_name] = dpath.util.values(query_response, values_glob)

    return response


async def select_values(client: influx.QueryClient, **kwargs) -> dict:
    """Selects data from Influx."""
    params = await configure_params(client, **kwargs)
    query = build_query(params)
    return await run_query(client, query, params)


async def configure_db(client: influx.QueryClient) -> dict:
    async def create_policy(name: str, duration: str, shard_duration: str):
        with suppress(InfluxDBError):
            await client.query(f' \
                CREATE RETENTION POLICY {name} \
                ON {influx.DEFAULT_DATABASE} \
                DURATION {duration} \
                REPLICATION 1 \
                SHARD DURATION {shard_duration}')

        await client.query(f' \
            ALTER RETENTION POLICY {name} \
            ON {influx.DEFAULT_DATABASE} \
            DURATION {duration} \
            REPLICATION 1 \
            SHARD DURATION {shard_duration}')

    async def create_cquery(period: str, source: str):
        with suppress(InfluxDBError):
            await client.query(
                f'DROP CONTINUOUS QUERY cq_downsample_{period} ON {influx.DEFAULT_DATABASE}')

        await client.query(f' \
            CREATE CONTINUOUS QUERY cq_downsample_{period} ON {influx.DEFAULT_DATABASE} \
            BEGIN \
                SELECT mean(*) AS m, sum(/(m_)*{influx.COMBINED_POINTS_FIELD}/) as m \
                INTO {influx.DEFAULT_DATABASE}.downsample_{period}.:MEASUREMENT \
                FROM {influx.DEFAULT_DATABASE}.{source}./.*/ \
                GROUP BY time({period}),* \
            END;')

    await create_policy('autogen', '1d', '6h')
    await create_policy('downsample_1m', 'INF', '1w')
    await create_policy('downsample_10m', 'INF', '1w')
    await create_policy('downsample_1h', 'INF', '4w')
    await create_policy('downsample_6h', 'INF', '4w')

    await create_cquery('1m', 'autogen')
    await create_cquery('10m', 'downsample_1m')
    await create_cquery('1h', 'downsample_10m')
    await create_cquery('6h', 'downsample_1h')

    return await client.query(f' \
        SHOW DATABASES; \
        SHOW RETENTION POLICIES ON {influx.DEFAULT_DATABASE}; \
        SHOW CONTINUOUS QUERIES; \
        ')

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
                approx_points:
                    type: int
                    required: false
                    example: 100
    """
    return await _do_with_handler(select_values, request)


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
