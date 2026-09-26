"""
Migrates the history of a legacy database into the dense and long-term databases.

When brewblox-ctl updates to the dense setup, it renames the single database of release 1 to
victoria-legacy and starts this job. The job runs in the background, and resumes at startup.
The datastore (`brewblox-history`/`migration`) holds what it started with and its outcome;
its progress is in the databases.

The migration is best effort: the migrated history is past brews. Live capture does not depend on it.

- seed: the last dense_days of raw samples, a day at a time back from where the legacy samples end,
  streamed from the legacy database's native export into the dense database. Short graphs of those days read dense.
  A restart seeds again: the dense database keeps one of a sample written twice.
- walk: the averages of every sparse_interval of the legacy history, a chunk at a time back to earliest,
  into the long-term database. Each chunk's import carries its marker (victoria.MIGRATION_MARKER, at its end,
  labelled with the job): a restart averages the chunks without one. After a chunk, the job pauses as long as
  the chunk took: it takes at most half the capacity. The walk counts the markers again, and averages the chunks
  without one again, until each has one or was tried MAX_ATTEMPTS times, also when that raised (older than the
  long-term retention, or unreadable): those are listed as lost. So are the legacy series without averages.
  A marker does not prove its chunk is on disk: the long-term database writes an import to disk in parts,
  and a crash can keep the marker while losing rows of its chunk. That loss goes unnoticed.
- done. The user removes the legacy database with brewblox-ctl.

Once a migration is planned, the downsampler imports nothing at or before where the legacy samples end
(VictoriaClient.legacy_end): the walk averages that time, and the dense database may hold a half-seeded day there.
The job logs errors and goes on after a pause: the service also serves the datastore.
"""

import asyncio
import logging
import math
import secrets
import time
from collections import Counter
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from contextvars import ContextVar

import httpx
from pydantic import ValidationError

from . import redis, utils, victoria
from .downsample import import_lines, marker_line
from .models import SEARCHABLE_DELAY, DatastoreValue, MigrationArgs, MigrationState, MigrationStatus
from .planner import chunk_seconds, quote, seconds

LOGGER = logging.getLogger(__name__)
LOGGER.addFilter(utils.DuplicateFilter())

CV: ContextVar['Migrator'] = ContextVar('migrate.migrator')

NAMESPACE = 'brewblox-history'
DOC_ID = 'migration'

DAY = 24 * 3600
# The pause after a failure, doubled after each next one up to the maximum (s)
RETRY_MIN = 1.0
RETRY_MAX = 60.0
# After averaging a chunk, the job pauses this many times as long as the chunk took
PAUSE_FACTOR = 1.0
# A chunk still without marker after this many tries is lost
MAX_ATTEMPTS = 2
# The legacy database gets no new samples: none of its points needs replacing
LEGACY_LATENCY_OFFSET = 0.001


class MigrationConflictError(Exception):
    """The request conflicts with the state of the migration."""


def chunk_ends(legacy_end: int, earliest: int, chunk: int) -> list[int]:
    """The ends of the walk's chunks, newest first. The oldest starts at earliest, and may be shorter."""
    return list(range(legacy_end, earliest, -chunk))


def planned_earliest(args: MigrationArgs, interval: int) -> int:
    """The start of the walk: an interval before the one holding earliest, so that a sample at earliest counts."""
    earliest = math.floor(args.earliest.timestamp())
    return earliest - earliest % interval - interval


def marker_selector(state: MigrationState) -> str:
    """The markers of this job: another job's are not its progress."""
    return f'{{__name__={quote(victoria.MIGRATION_MARKER)},migration={quote(state.job)}}}'


async def legacy_last(source: httpx.AsyncClient, now: int, earliest: int) -> float | None:
    """Unix seconds of the newest legacy sample after earliest - 1, looking back a day at a time.
    The per-day index tells which day has samples, without reading them;
    the migration usually starts right after the update, so the first day has them."""
    day_end = now
    while day_end > earliest:
        window = min(DAY, day_end - earliest)
        if await victoria.has_series(source, day_end - window, day_end):
            last = await victoria.last_timestamp(source, '{__name__!=""}', window + 1, day_end)
            if last is not None:
                return last
        day_end -= DAY
    return None


async def named_chunks(db: httpx.AsyncClient, chunks: AsyncIterator[bytes]) -> AsyncIterator[bytes]:
    """The chunks of a response, whose transport errors name its database:
    they are raised where another database reads them."""
    with victoria.named_errors(db):
        async for chunk in chunks:
            yield chunk


class Migrator:
    def __init__(self) -> None:
        self.state: MigrationState | None = None
        # Chunks with their marker, once counted by the running job
        self.chunks_done: int | None = None
        self.last_error: str | None = None
        self._loaded = False
        self._reset_job()
        self._task: asyncio.Task | None = None
        self._resume_task: asyncio.Task | None = None
        self._lock = asyncio.Lock()

    def _reset_job(self) -> None:
        """What a job keeps across the retries of its phases."""
        # Tries per chunk end, also those that raised
        self._attempts: Counter[int] = Counter()
        self._seed_cursor: int | None = None

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    def status(self) -> MigrationStatus | None:
        if self.state is None:
            return None
        return self._status(self.state, running=self.running)

    def _status(self, state: MigrationState, *, running: bool) -> MigrationStatus:
        done = state.chunks_total - len(state.lost_chunks) if state.phase == 'done' else self.chunks_done
        return MigrationStatus(**state.model_dump(), running=running, chunks_done=done, last_error=self.last_error)

    async def _set_legacy_end(self, legacy_end: int | None) -> None:
        """Tells the downsampler where the legacy samples end. It imports under the same lock:
        an import that is under way when a migration is planned is done first."""
        vic = victoria.CV.get()
        async with vic.legacy_lock:
            vic.legacy_end = legacy_end
            vic.legacy_end_known = True

    async def load(self) -> None:
        doc = await redis.CV.get().get(NAMESPACE, DOC_ID)
        if doc is None:
            self.state = None
        else:
            try:
                self.state = MigrationState.model_validate(doc.model_dump())
            except ValidationError:
                # The downsampler still needs to know where the legacy samples end
                legacy_end = doc.model_dump().get('legacy_end')
                await self._set_legacy_end(legacy_end if isinstance(legacy_end, int) else None)
                raise
        self._loaded = True
        await self._set_legacy_end(None if self.state is None else self.state.legacy_end)

    async def _load(self) -> MigrationState | None:
        try:
            await self.load()
        except ValidationError as ex:
            raise MigrationConflictError(f'The migration state is not valid, discard it: {utils.strex(ex)}') from ex
        return self.state

    async def save(self, state: MigrationState) -> None:
        doc = DatastoreValue.model_validate({'namespace': NAMESPACE, 'id': DOC_ID, **state.model_dump()})
        await redis.CV.get().set(doc)

    async def transition(self, state: MigrationState, **changes: object) -> None:
        """Saves the state with the changes, then makes them: if saving fails, the job is where it was,
        and tries again."""
        await self.save(state.model_copy(update=changes))
        for name, value in changes.items():
            setattr(state, name, value)

    async def plan(self, args: MigrationArgs) -> MigrationState:
        """A new migration: where the legacy samples end, and the chunks up to there."""
        config = utils.get_config()
        interval = seconds(config.sparse_interval)
        chunk = chunk_seconds(config)
        now = int(utils.now().timestamp())
        earliest = planned_earliest(args, interval)

        async with victoria.make_client(args.source_url) as source:
            last = await legacy_last(source, now, earliest)
        # The end of the interval that holds the last sample; without samples, there is nothing to do
        legacy_end = earliest if last is None else math.ceil(last / interval) * interval

        state = MigrationState(
            phase='seed',
            job=secrets.token_hex(6),
            source_url=args.source_url,
            earliest=earliest,
            dense_days=args.dense_days,
            sparse_interval=interval,
            chunk=chunk,
            legacy_last=last,
            legacy_end=legacy_end,
            chunks_total=len(chunk_ends(legacy_end, earliest, chunk)),
            started=now,
        )
        LOGGER.info(
            f'Migrating {args.source_url} from {utils.format_datetime(earliest, "ISO8601")}'
            f' to {utils.format_datetime(legacy_end, "ISO8601")}: {state.chunks_total} chunks'
        )
        return state

    def _check_interval(self, state: MigrationState) -> None:
        config = utils.get_config()
        if state.sparse_interval != seconds(config.sparse_interval):
            # Averages at another interval would mix two grids in the long-term database
            raise MigrationConflictError(
                f'The migration started with sparse_interval {state.sparse_interval}s,'
                f' now it is {seconds(config.sparse_interval)}s'
            )

    def _run(self, state: MigrationState) -> MigrationStatus:
        self._reset_job()
        self.chunks_done = None
        self._task = asyncio.create_task(self.run(state))
        return self._status(state, running=True)

    async def start(self, args: MigrationArgs) -> MigrationStatus:
        """Starts a migration, or resumes one that is not done.
        It keeps the earliest it started with: brewblox-ctl may ask for a later one each time."""
        config = utils.get_config()
        async with self._lock:
            if not config.dense_enabled:
                raise MigrationConflictError('The migration needs the dense database')
            if self.running:
                raise MigrationConflictError('The migration is running')
            if args.earliest.timestamp() > utils.now().timestamp():
                raise MigrationConflictError('The migration cannot start after now')

            state = await self._load()
            if state is None:
                state = await self.plan(args)
            elif state.phase == 'done':
                raise MigrationConflictError('The migration is done')
            elif (state.source_url, state.dense_days) != (args.source_url, args.dense_days):
                raise MigrationConflictError(
                    f'A migration from {state.source_url} is not done: discard it to start another one'
                )
            else:
                self._check_interval(state)
            state = state.model_copy(update={'cancelled': False})
            await self.save(state)
            self.state = state
            await self._set_legacy_end(state.legacy_end)
            self.last_error = None
            return self._run(state)

    async def cancel(self, *, discard: bool = False) -> MigrationStatus | None:
        """Stops the migration until it is started again, or discards it: its state is removed."""
        async with self._lock:
            # Not the resume at startup: if this request fails, it still reads the state
            await self._stop_job()
            if discard:
                await redis.CV.get().delete(NAMESPACE, DOC_ID)
                self.state = None
                await self._set_legacy_end(None)
                return None
            state = self.state if self._loaded else await self._load()
            if state is not None and state.phase != 'done':
                await self.transition(state, cancelled=True)
            return self.status()

    async def _stop_job(self) -> None:
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

    async def stop(self) -> None:
        """At shutdown: the job, and the resume if it still waits for the datastore."""
        if self._resume_task is not None:
            self._resume_task.cancel()
            await asyncio.gather(self._resume_task, return_exceptions=True)
            self._resume_task = None
        await self._stop_job()

    def start_resume(self) -> None:
        self._resume_task = asyncio.create_task(self.resume())

    async def resume(self) -> None:
        """At startup: continues a migration that is not done, unless it was cancelled.
        Waits for the datastore, unless a request started the migration first."""
        retry = RETRY_MIN
        while True:
            async with self._lock:
                if self.running:
                    return
                try:
                    await self.load()
                except ValidationError as ex:
                    LOGGER.error(f'The migration state is not valid, and not resumed: {utils.strex(ex)}')
                    return
                except Exception as ex:  # noqa: BLE001
                    LOGGER.warning(f'Migration state not read, trying again: {utils.strex(ex)}')
                else:
                    self._resume()
                    return
            await asyncio.sleep(retry)
            retry = min(retry * 2, RETRY_MAX)

    def _resume(self) -> None:
        state = self.state
        if state is None or state.phase == 'done' or state.cancelled:
            return
        try:
            self._check_interval(state)
        except MigrationConflictError as ex:
            self.last_error = str(ex)
            LOGGER.error(f'The migration is not resumed: {self.last_error}')
            return
        LOGGER.info(f'Resuming the migration ({state.phase})')
        self._run(state)

    async def check_legacy_end(self, source: httpx.AsyncClient, state: MigrationState) -> None:
        """The legacy database must not have samples after the last one the migration found:
        after a rollback, release 1 writes to it again."""
        found = state.legacy_last
        since = state.earliest if found is None else math.floor(found)
        last = await legacy_last(source, int(utils.now().timestamp()), since)
        if last is not None and (found is None or last > found):
            raise MigrationConflictError(
                'The legacy database has samples after the last one the migration found: discard it to start again'
            )

    async def run(self, state: MigrationState) -> None:
        retry = RETRY_MIN
        async with victoria.make_client(state.source_url) as source:
            checked = False
            while state.phase != 'done':
                try:
                    if not checked:
                        await self.check_legacy_end(source, state)
                        checked = True
                    if state.phase == 'seed':
                        await self.seed(source, state)
                    else:
                        await self.walk(source, state)
                    retry = RETRY_MIN
                except MigrationConflictError as ex:
                    self.last_error = str(ex)
                    LOGGER.error(f'The migration stopped: {self.last_error}')
                    return
                # Logged, and tried again: the service also serves the datastore
                except Exception as ex:  # noqa: BLE001
                    self.last_error = utils.strex(ex)
                    LOGGER.error(f'Migration failed ({state.phase}), trying again: {self.last_error}')
                    await asyncio.sleep(retry)
                    retry = min(retry * 2, RETRY_MAX)

    async def seed(self, source: httpx.AsyncClient, state: MigrationState) -> None:
        """Copies the last dense_days of raw samples into the dense database, a day at a time backwards."""
        config = utils.get_config()
        vic = victoria.CV.get()
        # The dense database drops older samples
        now = int(utils.now().timestamp())
        limit = max(state.legacy_end - state.dense_days * DAY, now - seconds(config.dense_retention))

        cursor = state.legacy_end if self._seed_cursor is None else self._seed_cursor
        while cursor > limit:
            start = max(cursor - DAY, limit)
            # Start and end included: the dense database keeps one of a sample exported twice
            params = {'match[]': '{__name__!=""}', 'start': start, 'end': cursor}
            with victoria.named_errors(source):
                async with source.stream('GET', '/api/v1/export/native', params=params) as resp:
                    if resp.is_error:
                        await resp.aread()
                        resp.raise_for_status()
                    await vic.import_dense_native(named_chunks(source, resp.aiter_bytes()))
            cursor = self._seed_cursor = start
            # Short graphs of the seeded days read the dense database
            if vic.dense_since is not None:
                vic.dense_since = min(vic.dense_since, start)

        await self.transition(state, phase='walk')
        # Only a seed that failed continues where it was
        self._seed_cursor = None

    async def average_chunk(self, source: httpx.AsyncClient, state: MigrationState, end: int) -> None:
        """Averages the chunk that ends at end into the long-term database, with its marker,
        then pauses as long as that took: live queries and writes go first."""
        began = time.monotonic()
        interval = state.sparse_interval
        start = max(end - state.chunk, state.earliest)
        # The database replaces points after its now with a copy of an older one. The last legacy interval
        # ends in the future when the migration starts right after the legacy samples end.
        wait = end + LEGACY_LATENCY_OFFSET - utils.now().timestamp()
        if wait > 0:
            await asyncio.sleep(wait)
            began = time.monotonic()
        body = await victoria.query_averages(source, start + interval, end, interval, LEGACY_LATENCY_OFFSET)
        # json.loads holds the GIL, but the conversion after it does not block the event loop
        lines = await asyncio.to_thread(import_lines, body)
        marker = marker_line(victoria.MIGRATION_MARKER, end, migration=state.job)
        await victoria.CV.get().import_archive('\n'.join([*lines, marker]))
        await asyncio.sleep((time.monotonic() - began) * PAUSE_FACTOR)

    async def missing_chunks(self, state: MigrationState) -> list[int]:
        """The ends of the chunks without marker, once the last markers are searchable."""
        await asyncio.sleep(SEARCHABLE_DELAY.total_seconds())
        marked = await victoria.CV.get().archive_timestamps(marker_selector(state), state.earliest, state.legacy_end)
        ends = chunk_ends(state.legacy_end, state.earliest, state.chunk)
        missing = [end for end in ends if end not in marked]
        self.chunks_done = len(ends) - len(missing)
        return missing

    async def walk(self, source: httpx.AsyncClient, state: MigrationState) -> None:
        """Averages the chunks without marker, newest first, until each has one or was tried MAX_ATTEMPTS times."""
        while True:
            missing = await self.missing_chunks(state)
            todo = [end for end in missing if self._attempts[end] < MAX_ATTEMPTS]
            if not todo:
                break
            again = [end for end in todo if self._attempts[end]]
            if again:
                LOGGER.warning(f'The long-term database lost {len(again)} chunks of the migration: averaging again')
            for end in todo:
                # Counted before: a chunk that raises counts too
                self._attempts[end] += 1
                await self.average_chunk(source, state, end)
                self.chunks_done = (self.chunks_done or 0) + 1

        if missing:
            LOGGER.warning(
                f'{len(missing)} chunks of the migration are lost:'
                ' older than the long-term retention, not readable, or the database failed again'
            )
        legacy = await victoria.series_names(source, state.earliest, state.legacy_end)
        archive = await victoria.CV.get().archive_series(state.earliest, state.legacy_end)
        await self.transition(
            state,
            phase='done',
            finished=int(utils.now().timestamp()),
            lost_chunks=missing,
            missing_series=sorted(legacy - archive),
        )
        self.last_error = None
        LOGGER.info(
            f'The migration is done: {state.chunks_total - len(missing)} chunks, {len(state.lost_chunks)} lost,'
            f' {len(state.missing_series)} legacy series without averages'
        )


def setup() -> None:
    CV.set(Migrator())


@asynccontextmanager
async def lifespan() -> AsyncIterator[None]:
    migrator = CV.get()
    if not utils.get_config().dense_enabled:
        yield
        return

    # Before the downsampler starts: it must not average before where the legacy samples end.
    # If the datastore is not there yet, resume() waits for it.
    with suppress(Exception):
        await migrator.load()
    migrator.start_resume()
    try:
        yield
    finally:
        await migrator.stop()
