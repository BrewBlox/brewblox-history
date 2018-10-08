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
        name: keys
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
    if 'keys' in request.query:
        params['keys'] = request.query.getall('keys')

    params = await queries.configure_params(client, **params)
    open_ended = 'duration' not in params and 'end' not in params

    async with sse_response(request) as resp:
        while True:
            try:
                query = queries.build_query(params)
                data = await queries.run_query(client, query, params)

                if data.get('values'):
                    await resp.send(json.dumps(data))
                    params['start'] = data['values'][-1][0] + 1

                if not open_ended:
                    break

                await asyncio.sleep(POLL_INTERVAL_S)

            except ConnectionResetError:  # pragma: no cover
                break

            except Exception as ex:
                LOGGER.warn(f'Exiting SSE with error: {type(ex).__name__}({ex})')
                break

    return resp
