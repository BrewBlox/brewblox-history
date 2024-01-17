"""
REST endpoints for TimeSeries queries
"""

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import APIRouter, Response, WebSocket, WebSocketDisconnect
from fastapi.encoders import jsonable_encoder
from fastapi.responses import StreamingResponse

from brewblox_history import utils, victoria
from brewblox_history.models import (PingResponse, TimeSeriesCsvQuery,
                                     TimeSeriesFieldsQuery, TimeSeriesMetric,
                                     TimeSeriesMetricsQuery,
                                     TimeSeriesMetricStreamData,
                                     TimeSeriesRange, TimeSeriesRangesQuery,
                                     TimeSeriesRangeStreamData,
                                     TimeSeriesStreamCommand)

CSV_CHUNK_SIZE = pow(2, 15)

LOGGER = logging.getLogger(__name__)
LOGGER.addFilter(utils.DuplicateFilter())

router = APIRouter(prefix='/timeseries', tags=['TimeSeries'])


@router.get('/ping')
async def timeseries_ping(response: Response) -> PingResponse:
    """
    Ping the Victoria Metrics database.
    """
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate, proxy-revalidate, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    await victoria.CV.get().ping()
    return PingResponse()


@router.post('/fields')
async def timeseries_fields(query: TimeSeriesFieldsQuery) -> list[str]:
    """
    List available fields in the database.
    """
    fields = await victoria.CV.get().fields(query)
    return fields


@router.post('/ranges')
async def timeseries_ranges(query: TimeSeriesRangesQuery) -> list[TimeSeriesRange]:
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
    """
    ranges = [v.model_dump(by_alias=True)
              for v in await victoria.CV.get().ranges(query)]
    return ranges


@router.post('/metrics')
async def timeseries_metrics(query: TimeSeriesMetricsQuery) -> list[TimeSeriesMetric]:
    """
    Get individual metrics from the database.
    """
    metrics = [v.model_dump()
               for v in await victoria.CV.get().metrics(query)]
    return metrics


@router.post('/csv')
async def timeseries_csv(response: Response, query: TimeSeriesCsvQuery):
    """
    Get value ranges formatted as CSV stream from the database.
    """
    async def generate():
        buffer = ''
        async for line in victoria.CV.get().csv(query):  # pragma: no branch
            buffer = f'{buffer}{line}\n'
            if len(buffer) >= CSV_CHUNK_SIZE:
                yield buffer.encode()
                buffer = ''

        # flush remainder
        yield buffer.encode()

    return StreamingResponse(
        generate(),
        headers={
            'Content-Type': 'text/plain',
            'Access-Control-Allow-Origin': '*',
        })


@asynccontextmanager
async def protected(desc: str):
    try:
        yield
    except Exception as ex:
        LOGGER.error(f'{desc} error {utils.strex(ex)}')


async def _stream_ranges(ws: WebSocket, id: str, query: TimeSeriesRangesQuery):
    config = utils.get_config()
    open_ended = utils.is_open_ended(start=query.start,
                                     duration=query.duration,
                                     end=query.end)
    initial = True

    while True:
        async with protected('ranges query'):
            data = TimeSeriesRangeStreamData(
                initial=initial,
                ranges=await victoria.CV.get().ranges(query))

            await ws.send_json({
                'id': id,
                'data': jsonable_encoder(data, by_alias=True)
            })

            query.start = utils.now()
            query.duration = None
            initial = False

        if not open_ended:
            break

        await asyncio.sleep(config.ranges_interval.total_seconds())


async def _stream_metrics(ws: WebSocket, id: str, query: TimeSeriesMetricsQuery):
    config = utils.get_config()

    while True:
        async with protected('metrics push'):
            data = TimeSeriesMetricStreamData(
                metrics=await victoria.CV.get().metrics(query),
            )

            await ws.send_json({
                'id': id,
                'data': jsonable_encoder(data),
            })

        await asyncio.sleep(config.metrics_interval.total_seconds())


@router.websocket('/stream')
async def timeseries_stream(ws: WebSocket):
    """
    Open a WebSocket to stream values from the database as they are added.

    When the socket is open, it supports commands for ranges and metrics.
    Each command starts a separate stream, but all streams share the same socket.
    Streams are identified by a command-defined ID.
    """
    await ws.accept()
    streams: dict[str, asyncio.Task] = {}

    try:
        while True:
            msg = await ws.receive_text()
            try:
                cmd = TimeSeriesStreamCommand.model_validate_json(msg)

                existing: asyncio.Task = streams.pop(cmd.id, None)
                existing and existing.cancel()

                if cmd.command == 'ranges':
                    streams[cmd.id] = asyncio.create_task(
                        _stream_ranges(ws, cmd.id, cmd.query))

                elif cmd.command == 'metrics':
                    streams[cmd.id] = asyncio.create_task(
                        _stream_metrics(ws, cmd.id, cmd.query))

                elif cmd.command == 'stop':
                    pass  # We already removed any pre-existing task from streams

                # Pydantic validates commands
                # This path should never be reached
                else:  # pragma: no cover
                    raise NotImplementedError('Unknown command')

            except Exception as ex:
                LOGGER.error(f'Stream read error {utils.strex(ex)}')
                await ws.send_json({
                    'error': utils.strex(ex),
                    'message': msg,
                })

    except WebSocketDisconnect:  # pragma: no cover
        pass

    finally:
        # Coverage complains about next line -> exit not being covered
        for task in streams.values():  # pragma: no cover
            task.cancel()
        await asyncio.gather(*streams.values(),
                             return_exceptions=True)
