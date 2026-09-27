"""
REST endpoints for TimeSeries queries
"""

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import cast

from fastapi import APIRouter, HTTPException, Response, WebSocket, WebSocketDisconnect
from fastapi.encoders import jsonable_encoder
from fastapi.responses import StreamingResponse

from brewblox_history import downsample, migrate, planner, utils, victoria
from brewblox_history.models import (
    MigrationArgs,
    MigrationStatus,
    TimeSeriesCsvQuery,
    TimeSeriesFieldsQuery,
    TimeSeriesMetric,
    TimeSeriesMetricsQuery,
    TimeSeriesMetricStreamData,
    TimeSeriesPingResponse,
    TimeSeriesRange,
    TimeSeriesRangesQuery,
    TimeSeriesRangeStreamData,
    TimeSeriesStreamCommand,
)

CSV_CHUNK_SIZE = pow(2, 15)

LOGGER = logging.getLogger(__name__)
LOGGER.addFilter(utils.DuplicateFilter())

router = APIRouter(prefix='/timeseries', tags=['TimeSeries'])


@router.get('/ping')
async def timeseries_ping(response: Response) -> TimeSeriesPingResponse:
    """
    Ping the Victoria Metrics databases, and report how old the long-term database's averages are.
    """
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate, proxy-revalidate, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    await victoria.CV.get().ping()
    return TimeSeriesPingResponse(downsample_age=downsample.CV.get().age(int(utils.now().timestamp())))


@router.post('/fields')
async def timeseries_fields(query: TimeSeriesFieldsQuery) -> list[str]:
    """
    List available fields in the database.
    """
    return await victoria.CV.get().fields(query)


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
    The period ends query_latency (5s) before now at the latest: newer samples may not be searchable yet.
    """
    return await victoria.CV.get().ranges(query)


@router.post('/metrics')
async def timeseries_metrics(query: TimeSeriesMetricsQuery) -> list[TimeSeriesMetric]:
    """
    Get individual metrics from the database.
    """
    return await victoria.CV.get().metrics(query)


@router.post('/migrate')
async def timeseries_migrate_start(args: MigrationArgs) -> MigrationStatus:
    """
    Start migrating a legacy database into the dense and long-term databases, or resume the migration.
    It runs in the background; the status tells how far it is.
    """
    try:
        return await migrate.CV.get().start(args)
    except migrate.MigrationConflictError as ex:
        raise HTTPException(409, str(ex)) from ex


@router.get('/migrate')
async def timeseries_migrate_status() -> MigrationStatus | None:
    """
    Get the status of the migration, if there is one.
    """
    return migrate.CV.get().status()


@router.delete('/migrate')
async def timeseries_migrate_cancel(*, discard: bool = False) -> MigrationStatus | None:
    """
    Stop the migration until it is started again. With discard, its state is removed.
    """
    try:
        return await migrate.CV.get().cancel(discard=discard)
    except migrate.MigrationConflictError as ex:
        raise HTTPException(409, str(ex)) from ex


@router.post('/csv')
async def timeseries_csv(query: TimeSeriesCsvQuery) -> StreamingResponse:
    """
    Get value ranges formatted as CSV stream from the database.
    """

    async def generate() -> AsyncIterator[bytes]:
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
        },
    )


@asynccontextmanager
async def protected(desc: str) -> AsyncIterator[None]:
    # Logged, and the stream goes on
    try:
        yield
    except Exception as ex:  # noqa: BLE001
        LOGGER.error(f'{desc} error {utils.strex(ex)}')


async def _send_ranges(ws: WebSocket, stream_id: str, ranges: list[TimeSeriesRange], *, initial: bool) -> None:
    data = TimeSeriesRangeStreamData(initial=initial, ranges=ranges)
    await ws.send_json({'id': stream_id, 'data': jsonable_encoder(data, by_alias=True)})


async def _stream_ranges(ws: WebSocket, stream_id: str, query: TimeSeriesRangesQuery) -> None:
    """
    Sends the ranges once (initial). While the query is open-ended, sends the points after the last one sent
    every ranges_interval, if there are any: each point is sent once.
    A failed query or send is tried again at the next interval.
    When the clock went back, the stream starts over with the initial ranges of every field,
    also those without values: the UI then drops what it holds.
    """
    config = utils.get_config()
    vic = victoria.CV.get()
    open_ended = utils.is_open_ended(start=query.start, duration=query.duration, end=query.end)
    # Where follow-ups continue, once the initial ranges are sent
    follow: planner.FollowUp | None = None

    while True:
        async with protected('ranges query'):
            if follow is None:
                ranges, next_follow = await vic.initial_ranges(query)
                await _send_ranges(ws, stream_id, ranges, initial=True)
            else:
                ranges, next_follow = await vic.follow_up_ranges(query.fields, follow)
                if next_follow is None:
                    ranges, next_follow = await vic.initial_ranges(query, every_field=True)
                    await _send_ranges(ws, stream_id, ranges, initial=True)
                elif ranges:
                    await _send_ranges(ws, stream_id, ranges, initial=False)
            follow = next_follow

        if not open_ended:
            break

        await asyncio.sleep(config.ranges_interval.total_seconds())


async def _stream_metrics(ws: WebSocket, stream_id: str, query: TimeSeriesMetricsQuery) -> None:
    config = utils.get_config()

    while True:
        async with protected('metrics push'):
            data = TimeSeriesMetricStreamData(
                metrics=await victoria.CV.get().metrics(query),
            )

            await ws.send_json(
                {
                    'id': stream_id,
                    'data': jsonable_encoder(data),
                }
            )

        await asyncio.sleep(config.metrics_interval.total_seconds())


@router.websocket('/stream')
async def timeseries_stream(ws: WebSocket) -> None:
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

                existing = streams.pop(cmd.id, None)
                if existing:
                    existing.cancel()

                # The command's validator gave the query the command's type
                if cmd.command == 'ranges':
                    query = cast('TimeSeriesRangesQuery', cmd.query)
                    streams[cmd.id] = asyncio.create_task(_stream_ranges(ws, cmd.id, query))

                elif cmd.command == 'metrics':
                    query = cast('TimeSeriesMetricsQuery', cmd.query)
                    streams[cmd.id] = asyncio.create_task(_stream_metrics(ws, cmd.id, query))

                elif cmd.command == 'stop':
                    pass  # We already removed any pre-existing task from streams

                # Pydantic validates commands
                # This path should never be reached
                else:  # pragma: no cover
                    raise NotImplementedError('Unknown command')  # noqa: TRY301

            # Reported to the client, and the socket stays open
            except Exception as ex:  # noqa: BLE001
                LOGGER.error(f'Stream read error {utils.strex(ex)}')
                await ws.send_json(
                    {
                        'error': utils.strex(ex),
                        'message': msg,
                    }
                )

    except WebSocketDisconnect:  # pragma: no cover
        pass

    finally:
        # Coverage complains about next line -> exit not being covered
        for task in streams.values():  # pragma: no cover
            task.cancel()
        await asyncio.gather(*streams.values(), return_exceptions=True)
