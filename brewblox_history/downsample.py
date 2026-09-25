"""
Averages the dense database's raw samples into the long-term database.

One task, only with dense_enabled. Every downsample_interval it averages each sparse_interval
that ended at least downsample_lag ago, from its cursor on, in chunks of downsample_chunk,
and imports the averages into the long-term database, stamped at the end of their interval:
where ranges() puts its points at a step of sparse_interval.
Each interval is averaged once: the database keeps the biggest of two values for one timestamp,
not the latest.

The cursor is the end of the last interval averaged. Every import carries a marker series
(victoria.MARKER) stamped at the new cursor, and the cursor only advances after the import
succeeded: a database that fails makes it wait, and the task catches up from the dense
database later (within its retention). At startup the marker gives the cursor again.
The long-term database keeps imports in memory for a while (-inmemoryDataFlushInterval) and
loses them when it crashes: the marker then ends before the cursor, and the task averages
that time again.

Reads use the cursor too (VictoriaClient.cursor), once the averages up to it are searchable.
The task also looks, at startup and every hour, where the dense database's samples start
(VictoriaClient.dense_since), for when it was created or wiped within its retention.

The task never stops on an error: the service also serves the datastore.
"""

import asyncio
import json
import logging
import math
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from contextvars import ContextVar

from . import utils, victoria
from .models import SEARCHABLE_DELAY
from .planner import seconds

LOGGER = logging.getLogger(__name__)
LOGGER.addFilter(utils.DuplicateFilter())

CV: ContextVar['Downsampler'] = ContextVar('downsample.downsampler')

HOUR = 3600
DAY = 24 * HOUR

MARKER_SELECTOR = f'{{__name__="{victoria.MARKER}"}}'

# How often to look again where the dense database's samples start
DENSE_SINCE_REFRESH = HOUR


def import_lines(body: bytes) -> list[str]:
    """Converts a query_range response to /api/v1/import JSON lines, with ms timestamps.
    Points that are not finite are left out: the database would skip their whole line."""
    lines = []
    for result in json.loads(body)['data']['result']:
        points = [(round(float(t) * 1000), float(v)) for t, v in result['values']]
        points = [(t, v) for t, v in points if math.isfinite(v)]
        if points:
            line = {
                'metric': result['metric'],
                'values': [v for _, v in points],
                'timestamps': [t for t, _ in points],
            }
            lines.append(json.dumps(line))
    return lines


def marker_line(cursor: int) -> str:
    return json.dumps({'metric': {'__name__': victoria.MARKER}, 'values': [cursor], 'timestamps': [cursor * 1000]})


def fmt(timestamp: int) -> str:
    return utils.format_datetime(timestamp, 'ISO8601')


class Downsampler:
    def __init__(self) -> None:
        # End of the last interval averaged into the long-term database (Unix s)
        self.cursor: int | None = None
        # When the task started (Unix s): the lag counts from there until the cursor is known
        self.started: int | None = None
        # The newest cursor the marker must reach: imported with it, and searchable
        self.marked: int | None = None
        self._dense_since_at: int | None = None
        self._lagging = False
        self._pending: set[asyncio.TimerHandle] = set()

    def age(self, now: int) -> int | None:
        """Seconds since the end of the last averages, or since the task started if not known yet."""
        since = self.cursor if self.cursor is not None else self.started
        return None if since is None else now - since

    def _set_read_cursor(self, cursor: int | None, *, force: bool = False) -> None:
        vic = victoria.CV.get()
        if force or vic.cursor is None:
            vic.cursor = cursor
        else:
            vic.cursor = max(vic.cursor, cursor)

    def _publish_cursor(self, cursor: int, delay: float) -> None:
        """Reads use the cursor once the averages up to it are searchable."""

        def publish() -> None:
            self._pending.discard(handle)
            self._set_read_cursor(cursor)
            self.marked = cursor if self.marked is None else max(self.marked, cursor)

        handle = asyncio.get_running_loop().call_later(delay, publish)
        self._pending.add(handle)

    def stop(self) -> None:
        for handle in self._pending:
            handle.cancel()
        self._pending.clear()

    async def find_dense_since(self, now: int) -> int | None:
        """Where the dense database's samples start, if it was created or wiped within its retention.

        The first day with any series comes from the per-day index, without reading samples;
        then the first hour of it with a sample."""
        config = utils.get_config()
        vic = victoria.CV.get()
        start = now - seconds(config.dense_retention)

        day = start - start % DAY
        first = None
        while first is None and day <= now:
            day_start = max(day, start)
            day_end = min(day + DAY, now)
            # The index covers whole days: up to the day's last second, or the next day counts too
            if await vic.has_series('dense', day_start, min(day + DAY - 1, now)):
                hour = day_start
                while first is None and hour < day_end:
                    # (hour - 1, hour + HOUR]: a sample on the hour is not missed
                    first = await vic.first_timestamp('dense', HOUR + 1, min(hour + HOUR, now))
                    hour += HOUR
            day += DAY

        if first is None:
            # No samples yet: they start now
            return now
        since = math.floor(first)
        return since if since > now - seconds(config.dense_retention) + seconds(config.dense_margin) else None

    async def discover_cursor(self, now: int, dense_since: int | None) -> int:
        """Where the averages in the long-term database end, from the marker,
        or where the dense database's samples start if that is later."""
        config = utils.get_config()
        vic = victoria.CV.get()
        interval = seconds(config.sparse_interval)
        # The first interval that is whole inside the dense retention...
        retention_start = now - seconds(config.dense_retention)
        retention_start += -retention_start % interval
        # ... or the one holding the dense database's first sample, if later
        start = retention_start if dense_since is None else max(retention_start, dense_since - dense_since % interval)

        last = await vic.last_timestamp('archive', MARKER_SELECTOR, seconds(config.dense_retention), now)
        if last is None:
            # Older than the dense retention, or none at all: the index knows without reading samples.
            # The database takes start=0 as not set (the last day): 1 is the whole index.
            if await vic.has_series('archive', 1, now, MARKER_SELECTOR):
                LOGGER.warning(
                    'The long-term database has averages from before the dense retention, and the dense database'
                    f' has samples from {fmt(start)}: the time between is not averaged'
                )
            else:
                LOGGER.info(f'No averages in the long-term database: averaging from {fmt(start)}')
            return start

        # Markers are on the grid of the interval, unless it was changed
        cursor = math.floor(last)
        cursor -= cursor % interval
        self.marked = cursor
        if cursor < start:
            LOGGER.info(f'The dense database has samples from {fmt(start)}: averaging from there')
            return start
        return cursor

    async def check_archive(self, now: int) -> None:
        """After a crash, the long-term database may have lost the last imports it held in memory.
        The marker then ends before where it was seen: average again from the marker."""
        vic = victoria.CV.get()
        marked = self.marked
        if marked is None:
            return
        # A publish during the query may raise self.marked: compare with the value queried for
        last = await vic.last_timestamp('archive', MARKER_SELECTOR, max(now - marked, 0) + 1, now)
        if last is not None and last >= marked:
            return

        LOGGER.warning(f'The long-term database lost averages up to {fmt(marked)}: averaging them again')
        # Reads must not count on the lost averages, and imports waiting to become searchable may be lost too
        self.stop()
        self.cursor = None
        self.marked = None
        self._set_read_cursor(None, force=True)

    async def downsample(self, now: int) -> None:
        """Averages every interval that ended at least downsample_lag ago, from the cursor on."""
        config = utils.get_config()
        vic = victoria.CV.get()
        interval = seconds(config.sparse_interval)
        # At least one interval, and whole intervals
        chunk = max(seconds(config.downsample_chunk), interval)
        chunk -= chunk % interval
        target = now - math.ceil(config.downsample_lag.total_seconds())
        target -= target % interval

        while self.cursor < target:
            end = min(self.cursor + chunk, target)
            body = await vic.averages(self.cursor + interval, end, interval)
            # json.loads holds the GIL, but the conversion after it does not block the event loop.
            # downsample_chunk keeps the parse short.
            lines = await asyncio.to_thread(import_lines, body)
            # Every import carries the marker, also when there was nothing to average
            await vic.import_archive('\n'.join([*lines, marker_line(end)]))
            self.cursor = end
            self._publish_cursor(end, SEARCHABLE_DELAY.total_seconds())

    def max_lag(self) -> int:
        """downsample_max_lag, or more: keeping up, the averages end up to
        downsample_lag + sparse_interval + downsample_interval ago, and the warning must not come and go with that."""
        config = utils.get_config()
        keeping_up = seconds(config.downsample_lag + 2 * config.sparse_interval + config.downsample_interval)
        return max(seconds(config.downsample_max_lag), keeping_up)

    def check_lag(self, now: int) -> None:
        """Warns once while the averages end more than max_lag() ago."""
        age = self.age(now)
        if age is None:
            return
        if age > self.max_lag() and not self._lagging:
            self._lagging = True
            LOGGER.warning(f'Downsampling is behind: the long-term database has averages up to {age} s ago')
        elif age <= self.max_lag() and self._lagging:
            self._lagging = False
            LOGGER.info('Downsampling caught up')

    async def tick(self) -> None:
        now = int(utils.now().timestamp())
        vic = victoria.CV.get()
        if self._dense_since_at is None or now - self._dense_since_at >= DENSE_SINCE_REFRESH:
            vic.dense_since = await self.find_dense_since(now)
            self._dense_since_at = now
        await self.check_archive(now)
        if self.cursor is None:
            self.cursor = await self.discover_cursor(now, vic.dense_since)
            # Found in the database: already searchable
            self._set_read_cursor(self.cursor, force=True)
        await self.downsample(now)

    async def run(self) -> None:
        config = utils.get_config()
        self.started = int(utils.now().timestamp())
        while True:
            try:
                await self.tick()
            # Logged, and the task goes on: the service also serves the datastore
            except Exception as ex:  # noqa: BLE001
                LOGGER.error(f'Downsampling failed: {utils.strex(ex)}')
            self.check_lag(int(utils.now().timestamp()))
            await asyncio.sleep(config.downsample_interval.total_seconds())


def setup() -> None:
    CV.set(Downsampler())


@asynccontextmanager
async def lifespan() -> AsyncIterator[None]:
    config = utils.get_config()
    downsampler = CV.get()
    if not config.dense_enabled:
        yield
        return

    task = asyncio.create_task(downsampler.run())
    try:
        yield
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        downsampler.stop()
