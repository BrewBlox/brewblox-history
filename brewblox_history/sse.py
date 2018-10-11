"""
Server-sent events implementation for relaying eventbus messages to front end
"""

import asyncio
import json

from aiohttp import web
from aiohttp_sse import sse_response
from brewblox_service import brewblox_logger

from brewblox_history import influx, queries

LOGGER = brewblox_logger(__name__)
routes = web.RouteTableDef()

POLL_INTERVAL_S = 1


def setup(app: web.Application):
    app.router.add_routes(routes)


def _check_open_ended(params: dict) -> bool:
    time_args = [bool(params.get(k)) for k in ('start', 'duration', 'end')]
    return time_args in [
        [False, False, False],
        [True, False, False],
        [False, True, False],
    ]


@routes.get('/sse/values')
async def subscribe(request: web.Request) -> web.Response:
    """
    ---
    tags:
    - History
    summary: subscribe to InfluxDB updates
    operationId: history.sse.values
    produces:
    - application/json
    parameters:
    -
        in: query
        name: database
        schema:
            type: string
            required: false
            example: "brewblox"
    -
        in: query
        name: measurement
        schema:
            type: string
            required: true
            example: "spark"
    -
        in: query
        name: fields
        schema:
            type: list
            required: false
            example: ["*"]
    -
        in: query
        name: approx_points
        schema:
            type: int
            required: false
            example: 100
    -
        in: query
        name: start
        schema:
            type: string
            required: false
    -
        in: query
        name: duration
        schema:
            type: string
            required: false
    -
        in: query
        name: end
        schema:
            type: string
            required: false
    """
    client = influx.get_client(request.app)
    params = {k: request.query.get(k) for k in [
        'database',
        'measurement',
        'approx_points',
        'start',
        'duration',
        'end',
    ] if k in request.query}
    if 'fields' in request.query:
        params['fields'] = request.query.getall('fields')

    params = await queries.configure_params(client, **params)
    open_ended = _check_open_ended(params)

    async with sse_response(request) as resp:
        while True:
            try:
                query = queries.build_query(params)
                data = await queries.run_query(client, query, params)

                if data.get('values'):
                    await resp.send(json.dumps(data))
                    # Reset time frame for subsequent updates
                    params['start'] = data['values'][-1][0] + 1
                    params.pop('duration', None)

                if not open_ended:
                    break

                await asyncio.sleep(POLL_INTERVAL_S)

            except Exception as ex:
                msg = f'Exiting SSE with error: {type(ex).__name__}({ex})'
                LOGGER.warn(msg)
                break

    return resp
