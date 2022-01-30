"""
REST endpoints for TimeSeries queries
"""

import asyncio
from contextlib import asynccontextmanager

from aiohttp import web
from aiohttp_pydantic import PydanticView
from aiohttp_pydantic.oas.typing import r200
from brewblox_service import brewblox_logger, strex

from brewblox_history import socket_closer, utils, victoria
from brewblox_history.models import (TimeSeriesCsvQuery, TimeSeriesFieldsQuery,
                                     TimeSeriesMetric, TimeSeriesMetricsQuery,
                                     TimeSeriesMetricStreamData,
                                     TimeSeriesRange, TimeSeriesRangesQuery,
                                     TimeSeriesRangeStreamData,
                                     TimeSeriesStreamCommand)

LOGGER = brewblox_logger(__name__, True)
routes = web.RouteTableDef()


@asynccontextmanager
async def protected(desc: str):
    try:
        yield
    except Exception as ex:
        LOGGER.error(f'{desc} error {strex(ex)}')


class VictoriaView(PydanticView):
    def __init__(self, request: web.Request) -> None:
        super().__init__(request)
        self.victoria = victoria.fget(request.app)


@routes.view('/timeseries/ping')
class PingView(VictoriaView):
    async def get(self) -> r200[dict]:
        """
        Ping the Victoria Metrics database.

        Tags: TimeSeries
        """
        await self.victoria.ping()
        return utils.json_response(
            data={'ok': True},
            headers={
                'Cache-Control': 'no-cache',
                'Pragma': 'no-cache',
                'Expires': '0',
            })


@routes.view('/timeseries/fields')
class FieldsView(VictoriaView):
    async def post(self, args: TimeSeriesFieldsQuery) -> r200[list[str]]:
        """
        List available fields in the database.

        Tags: TimeSeries
        """
        return utils.json_response(
            await self.victoria.fields(args)
        )


@routes.view('/timeseries/ranges')
class RangesView(VictoriaView):
    async def post(self, args: TimeSeriesRangesQuery) -> r200[list[TimeSeriesRange]]:
        """
        Get value ranges from the database.

        The start, end, and duration arguments can be used to set the period.
        At most two of them may be set. The combinations are: <br>
        - none:               between now()-1d and now() <br>
        - start + duration:   between start and start + duration <br>
        - start + end:        between start and end <br>
        - duration + end:     between end - duration and end <br>
        - start:              between start and now() <br>
        - duration:           between now() - duration and now() <br>
        - end:                between end-1d and end <br>

        Tags: TimeSeries
        """
        return utils.json_response([
            v.dict(by_alias=True) for v in
            await self.victoria.ranges(args)
        ])


@routes.view('/timeseries/metrics')
class MetricsView(VictoriaView):
    async def post(self, args: TimeSeriesMetricsQuery) -> r200[list[TimeSeriesMetric]]:
        """
        Get individual metrics from the database.

        Tags: TimeSeries
        """
        return utils.json_response([
            v.dict() for v in
            await self.victoria.metrics(args)
        ])


@routes.view('/timeseries/csv')
class CsvView(VictoriaView):
    async def post(self, args: TimeSeriesCsvQuery) -> web.StreamResponse:
        """
        Get value ranges formatted as CSV stream from the database.

        Tags: TimeSeries
        """
        response = web.StreamResponse(
            status=200,
            reason='OK',
            headers={
                'Content-Type': 'text/plain',
                'Access-Control-Allow-Origin': '*',
            }
        )
        await response.prepare(self.request)
        response.enable_chunked_encoding()

        async for line in self.victoria.csv(args):  # pragma: no branch
            await response.write(line.encode())
            await response.write('\n'.encode())

        await response.write_eof()
        return response


@routes.view('/timeseries/stream')
class StreamView(VictoriaView):

    def __init__(self, request: web.Request) -> None:
        super().__init__(request)
        self.app = request.app
        self.ws: web.WebSocketResponse

    async def _stream_ranges(self, id: str, query: TimeSeriesRangesQuery):
        interval = self.app['config']['ranges_interval']
        open_ended = utils.is_open_ended(start=query.start,
                                         duration=query.duration,
                                         end=query.end)
        initial = True

        while True:
            async with protected('ranges query'):
                await self.ws.send_json(
                    {
                        'id': id,
                        'data': TimeSeriesRangeStreamData(
                            initial=initial,
                            ranges=await self.victoria.ranges(query)
                        ).dict(by_alias=True),  # for the __name__ field
                    },
                    dumps=utils.json_dumps)

                query.start = utils.now()
                query.duration = None
                initial = False

            if not open_ended:
                break

            await asyncio.sleep(interval)

    async def _stream_metrics(self, id: str, query: TimeSeriesMetricsQuery):
        interval = self.app['config']['metrics_interval']

        while True:
            async with protected('metrics push'):
                await self.ws.send_json(
                    {
                        'id': id,
                        'data': TimeSeriesMetricStreamData(
                            metrics=await self.victoria.metrics(query),
                        ).dict(),
                    },
                    dumps=utils.json_dumps)

            await asyncio.sleep(interval)

    async def get(self) -> web.WebSocketResponse:
        """
        Open a WebSocket to stream values from the database as they are added.

        When the socket is open, it supports commands for ranges and metrics.
        Each command starts a separate stream, but all streams share the same socket.
        Streams are identified by a command-defined ID.

        Tags: TimeSeries
        """
        self.ws = web.WebSocketResponse()
        streams = {}

        try:
            await self.ws.prepare(self.request)
            socket_closer.add(self.app, self.ws)

            async for msg in self.ws:  # pragma: no branch
                try:
                    cmd = TimeSeriesStreamCommand.parse_raw(msg.data)

                    existing: asyncio.Task = streams.pop(cmd.id, None)
                    existing and existing.cancel()

                    if cmd.command == 'ranges':
                        query = TimeSeriesRangesQuery(**cmd.query)
                        streams[cmd.id] = asyncio.create_task(
                            self._stream_ranges(cmd.id, query))

                    elif cmd.command == 'metrics':
                        query = TimeSeriesMetricsQuery(**cmd.query)
                        streams[cmd.id] = asyncio.create_task(
                            self._stream_metrics(cmd.id, query))

                    elif cmd.command == 'stop':
                        pass  # We already removed any pre-existing task from streams

                    # Pydantic validates commands
                    # This path should never be reached
                    else:  # pragma: no cover
                        raise NotImplementedError('Unknown command')

                except Exception as ex:
                    LOGGER.error(f'Stream read error {strex(ex)}')
                    await self.ws.send_json({
                        'error': strex(ex),
                        'message': msg,
                    })

        finally:
            socket_closer.discard(self.app, self.ws)
            # Coverage complains about next line -> exit not being covered
            for task in streams.values():  # pragma: no cover
                task.cancel()

        return self.ws


def setup(app: web.Application):
    app.router.add_routes(routes)
