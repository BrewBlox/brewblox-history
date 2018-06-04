"""
Server-sent events implementation for relaying eventbus messages to front end
"""

import asyncio
import json
import time

from aiohttp import web
from aiohttp_sse import sse_response
from brewblox_history import builder, influx
from brewblox_service import brewblox_logger

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
            example: "brewblox"
            required: false
    -
        in: query
        name: measurement
        schema:
            type: string
            example: "spark"
            required: true
    -
        in: query
        name: keys
        schema:
            type: list
            required: false
            example: ["*"]
    """
    influx_client = influx.get_client(request.app)
    database = request.query.get('database')
    measurement = request.query.get('measurement')
    keys = []
    for k in request.query.getall('keys', ['*']):
        keys += k.split(',')

    async with sse_response(request) as resp:
        # Convert seconds to nanoseconds
        previous = int(time.time() * 1000 * 1000 * 1000)

        while True:
            try:
                await asyncio.sleep(POLL_INTERVAL_S)

                data = await builder.select_values(
                    influx_client,
                    database=database,
                    measurement=measurement,
                    keys=keys,
                    start=previous + 1
                )

                if data.get('values'):
                    await resp.send(json.dumps(data))
                    previous = data['values'][-1][0]

            except Exception as ex:
                LOGGER.warn(f'Exiting SSE with error: {ex}')
                break

    return resp
