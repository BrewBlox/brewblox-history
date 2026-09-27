"""
Tests brewblox_history.migrate
"""

import asyncio
import json
import logging
import re
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime, timedelta
from unittest.mock import Mock
from urllib.parse import parse_qs

import httpx
import pytest
from asgi_lifespan import LifespanManager
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient, Request, Response
from pytest_httpx import HTTPXMock
from pytest_mock import MockerFixture

from brewblox_history import app_factory, downsample, migrate, redis, timeseries_api, utils, victoria
from brewblox_history.models import DatastoreValue, MigrationArgs, MigrationState, MigrationStatus, ServiceConfig
from test.conftest import FakeDatastore

TESTED = migrate.__name__

# 2021-07-15 19:00 UTC, on the hour
NOW = 1626375600
HOUR = 3600
DAY = 24 * HOUR
LEGACY = 'http://legacy:8428/victoria-legacy'
# The job of new_state()
JOB = 'job'
# Where the walk starts and ends for args(), and the ends of its chunks
EARLIEST = NOW - 3 * HOUR - 60
LEGACY_END = NOW - 120
ENDS = [NOW - 120, NOW - 3720, NOW - 7320]


def params(request: Request) -> dict[str, str]:
    body = parse_qs(request.read().decode()) if request.method == 'POST' else parse_qs(request.url.query.decode())
    return {k: v[0] for k, v in body.items()}


def vector(value: float | None) -> Response:
    result = [] if value is None else [{'metric': {}, 'value': [NOW, str(value)]}]
    return Response(200, json={'status': 'success', 'data': {'resultType': 'vector', 'result': result}})


def at(timestamp: int) -> datetime:
    return datetime.fromtimestamp(timestamp, UTC)


class BrokenStream(httpx.AsyncByteStream):
    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield b'native'
        raise httpx.ReadError('connection reset')


class FakeLegacy:
    """The legacy database: its series have samples up to `last`."""

    def __init__(self) -> None:
        self.last: float | None = NOW - 125.5
        self.names = {'a', 'b'}
        self.broken = False
        # Queries to refuse, as while the database starts
        self.failures = 0
        # Called for every averages query
        self.on_query: Callable[[], None] = lambda: None
        # The ends of chunks whose averages the database refuses
        self.unreadable: set[int] = set()

    def query(self, request: Request) -> Response:
        # The newest sample in (time - window, time]
        if self.failures:
            self.failures -= 1
            return Response(503, text='starting')
        p = params(request)
        assert p['nocache'] == '1'
        window = int(p['query'].split('[')[1].split('s]')[0])
        t = int(p['time'])
        found = self.last is not None and t - window < self.last <= t
        return vector(self.last if found else None)

    def query_range(self, request: Request) -> Response:
        # Each series at every point, valued by its timestamp
        self.on_query()
        p = params(request)
        start, end, step = int(p['start']), int(p['end']), int(p['step'].rstrip('s'))
        if end in self.unreadable:
            return Response(422, text='cannot read')
        result = [
            {'metric': {'__name__': name}, 'values': [[t, str(t % 1000)] for t in range(start, end + 1, step)]}
            for name in sorted(self.names)
        ]
        return Response(200, json={'status': 'success', 'data': {'resultType': 'matrix', 'result': result}})

    def export_native(self, request: Request) -> Response:
        p = params(request)
        if self.broken:
            return Response(200, stream=BrokenStream())
        return Response(200, content=f'native {p["start"]}-{p["end"]};'.encode())

    def series(self, request: Request) -> Response:
        p = params(request)
        start, end = int(p['start']), int(p['end'])
        if 'limit' in p:
            # The per-day index: whole days
            found = self.last is not None and start - start % DAY <= self.last < end - end % DAY + DAY
        else:
            found = True
        names = sorted(self.names) if found else []
        return Response(200, json={'status': 'success', 'data': [{'__name__': n} for n in names]})


class FakeArchive:
    """The long-term database. It keeps no samples before `retention_start`. `events` has the imports in order."""

    def __init__(self) -> None:
        self.lines: list[dict] = []
        self.events: list[tuple] = []
        self.retention_start = 0
        self.failures = 0
        # Imports to accept and lose, as in a crash
        self.lose_imports = 0
        # Called for every import
        self.on_import: Callable[[], None] = lambda: None

    def import_(self, request: Request) -> Response:
        if self.failures:
            self.failures -= 1
            return Response(500, text='import failed')
        self.on_import()
        lines = [json.loads(line) for line in request.read().decode().splitlines()]
        kept = [line for line in lines if line['timestamps'][-1] >= self.retention_start * 1000]
        if self.lose_imports:
            self.lose_imports -= 1
            kept = []
        self.lines.extend(kept)
        self.events.append(('import', [(line['metric']['__name__'], line['timestamps'][-1] // 1000) for line in lines]))
        return Response(204)

    def markers(self, job: str) -> list[dict]:
        return [
            line for line in self.lines if line['metric'] == {'__name__': victoria.MIGRATION_MARKER, 'migration': job}
        ]

    @property
    def marked(self) -> list[int]:
        """The chunk ends with a marker of JOB."""
        return sorted(line['timestamps'][0] // 1000 for line in self.markers(JOB))

    def export(self, request: Request) -> Response:
        match = re.fullmatch(r'\{__name__="(.*)",migration="(.*)"\}', params(request)['match[]'])
        assert match is not None
        assert match[1] == victoria.MIGRATION_MARKER
        return Response(200, text='\n'.join(json.dumps(line) for line in self.markers(match[2])))

    def series(self, request: Request) -> Response:
        names = sorted({line['metric']['__name__'] for line in self.lines})
        return Response(200, json={'status': 'success', 'data': [{'__name__': n} for n in names]})


@pytest.fixture
def url(config: ServiceConfig) -> str:
    return f'{config.victoria_protocol}://{config.victoria_host}:{config.victoria_port}{config.victoria_path}'


@pytest.fixture
def dense_url(config: ServiceConfig) -> str:
    return f'{config.dense_protocol}://{config.dense_host}:{config.dense_port}{config.dense_path}'


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """The time, in Unix seconds: set clock[0] to move it."""
    clock = [NOW]
    monkeypatch.setattr(utils, 'now', lambda: at(clock[0]))
    return clock


@pytest.fixture
def datastore(mocker: MockerFixture) -> FakeDatastore:
    fake = FakeDatastore()
    mocker.patch.object(redis, 'CV').get.return_value = fake
    return fake


@pytest.fixture
def migrator(
    config: ServiceConfig,
    clock: list[int],
    datastore: FakeDatastore,
    monkeypatch: pytest.MonkeyPatch,
) -> migrate.Migrator:
    config.sparse_interval = timedelta(seconds=60)
    config.downsample_chunk = timedelta(hours=1)
    # No real pauses
    monkeypatch.setattr(migrate, 'PAUSE_FACTOR', 0)
    monkeypatch.setattr(migrate, 'SEARCHABLE_DELAY', timedelta())
    monkeypatch.setattr(migrate, 'RETRY_MIN', 0.001)
    monkeypatch.setattr(migrate, 'RETRY_MAX', 0.002)
    # Every job is JOB
    monkeypatch.setattr(migrate.secrets, 'token_hex', lambda nbytes: JOB if nbytes == 6 else None)
    victoria.setup()
    migrate.setup()
    return migrate.CV.get()


@pytest.fixture
def legacy(httpx_mock: HTTPXMock) -> FakeLegacy:
    fake = FakeLegacy()
    for path, method, callback in [
        ('/api/v1/query', 'POST', fake.query),
        ('/api/v1/query_range', 'POST', fake.query_range),
        ('/api/v1/series', 'POST', fake.series),
        ('/api/v1/export/native', 'GET', fake.export_native),
    ]:
        httpx_mock.add_callback(
            url=re.compile(re.escape(LEGACY + path) + r'(\?.*)?$'),
            method=method,
            callback=callback,
            is_reusable=True,
            is_optional=True,
        )
    return fake


@pytest.fixture
def archive(httpx_mock: HTTPXMock, url: str) -> FakeArchive:
    fake = FakeArchive()
    for path, callback in [
        ('/api/v1/import', fake.import_),
        ('/api/v1/export', fake.export),
        ('/api/v1/series', fake.series),
    ]:
        httpx_mock.add_callback(url=url + path, method='POST', callback=callback, is_reusable=True, is_optional=True)
    return fake


@pytest.fixture
def seeded(httpx_mock: HTTPXMock, dense_url: str) -> list[bytes]:
    """What the dense database's native import got, per request."""
    bodies: list[bytes] = []

    async def native_import(request: Request) -> Response:
        bodies.append(await request.aread())
        return Response(204)

    httpx_mock.add_callback(
        url=f'{dense_url}/api/v1/import/native',
        method='POST',
        callback=native_import,
        is_reusable=True,
        is_optional=True,
    )
    return bodies


def args(earliest: int = NOW - 3 * HOUR + 30, dense_days: int = 1, source_url: str = LEGACY) -> MigrationArgs:
    return MigrationArgs(source_url=source_url, earliest=at(earliest), dense_days=dense_days)


def new_state(**changes: object) -> MigrationState:
    """The state plan() gives for args(): three chunks back from where the legacy samples end."""
    state = MigrationState(
        phase='seed',
        job=JOB,
        source_url=LEGACY,
        earliest=EARLIEST,
        dense_days=1,
        sparse_interval=60,
        chunk=HOUR,
        legacy_last=NOW - 125.5,
        legacy_end=LEGACY_END,
        chunks_total=3,
        started=NOW,
    )
    return state.model_copy(update=changes)


def state_doc(state: MigrationState) -> DatastoreValue:
    return DatastoreValue.model_validate({'namespace': migrate.NAMESPACE, 'id': migrate.DOC_ID, **state.model_dump()})


def saved(datastore: FakeDatastore) -> MigrationState:
    return MigrationState.model_validate_json(datastore.docs[(migrate.NAMESPACE, migrate.DOC_ID)])


async def finish(migrator: migrate.Migrator) -> None:
    """Waits for the migration's task."""
    task = migrator._task
    assert task is not None
    await asyncio.wait_for(task, 1)


def migrate_source() -> AsyncClient:
    return victoria.make_client(LEGACY)


async def test_migrate(
    migrator: migrate.Migrator,
    datastore: FakeDatastore,
    legacy: FakeLegacy,
    archive: FakeArchive,
    seeded: list[bytes],
):
    # Where the legacy samples end: the end of the interval with the last one.
    # The start: an interval before the one holding earliest.
    status = await migrator.start(args())
    assert status == MigrationStatus(**new_state().model_dump(), running=True, chunks_done=None, last_error=None)
    assert saved(datastore) == new_state()
    assert (victoria.CV.get().legacy_end, victoria.CV.get().legacy_end_known) == (LEGACY_END, True)

    await finish(migrator)
    state = saved(datastore)
    assert (state.phase, state.finished, state.lost_chunks, state.missing_series) == ('done', NOW, [], [])
    assert migrator.status() == MigrationStatus(**state.model_dump(), running=False, chunks_done=3, last_error=None)

    # A day of raw samples, streamed into the dense database
    assert seeded == [f'native {LEGACY_END - DAY}-{LEGACY_END};'.encode()]
    # Averages in chunks back from where the legacy samples end, each with its marker
    marker = victoria.MIGRATION_MARKER
    assert archive.events == [
        ('import', [('a', end), ('b', end), (marker, end)]) for end in [NOW - 120, NOW - 3720, NOW - 7320]
    ]
    # Labelled with the job: its markers are its progress
    assert archive.markers(JOB) == [
        {'metric': {'__name__': marker, 'migration': JOB}, 'values': [end], 'timestamps': [end * 1000]}
        for end in [NOW - 120, NOW - 3720, NOW - 7320]
    ]
    first = archive.lines[0]
    assert first == {
        'metric': {'__name__': 'a'},
        'values': [float(t % 1000) for t in range(NOW - 3660, NOW - 119, 60)],
        'timestamps': [t * 1000 for t in range(NOW - 3660, NOW - 119, 60)],
    }
    # The oldest averages the interval that ends at earliest
    assert archive.lines[6]['timestamps'][0] == (NOW - 3 * HOUR) * 1000


async def test_legacy_last(migrator: migrate.Migrator, legacy: FakeLegacy, httpx_mock: HTTPXMock):
    # The per-day index tells which day to look at, back from now. It answers for whole days:
    # a window across two days may find no sample.
    legacy.last = NOW - DAY - 10.5
    state = await migrator.plan(args(earliest=NOW - 10 * DAY))
    assert (state.legacy_last, state.legacy_end) == (NOW - DAY - 10.5, NOW - DAY)
    assert [r.url.path.split('/')[-1] for r in httpx_mock.get_requests()] == ['series', 'query', 'series', 'query']

    # Without legacy samples: nothing to do, and no samples read
    legacy.last = None
    state = await migrator.plan(args(earliest=NOW - 3 * DAY + 1800))
    assert (state.legacy_last, state.legacy_end, state.chunks_total) == (None, NOW - 3 * DAY + 1740, 0)
    assert [r.url.path.split('/')[-1] for r in httpx_mock.get_requests()[4:]] == ['series'] * 3


async def test_start_refused(
    migrator: migrate.Migrator,
    config: ServiceConfig,
    datastore: FakeDatastore,
    legacy,
    archive,
    seeded,
):
    # Not twice, also when asked at the same time
    results = await asyncio.gather(migrator.start(args()), migrator.start(args()), return_exceptions=True)
    assert isinstance(results[0], MigrationStatus)
    assert isinstance(results[1], migrate.MigrationConflictError)
    assert 'running' in str(results[1])
    await migrator.stop()

    # Another source or dense_days while a migration is not done. A later earliest resumes it as it started:
    # brewblox-ctl may ask for a later one each time.
    with pytest.raises(migrate.MigrationConflictError, match=f'A migration from {LEGACY} is not done'):
        await migrator.start(args(source_url='http://other:8428/victoria-legacy'))
    with pytest.raises(migrate.MigrationConflictError, match='not done'):
        await migrator.start(args(dense_days=2))
    migrator.last_error = 'earlier failure'
    status = await migrator.start(args(earliest=NOW - HOUR))
    assert (status.earliest, status.last_error) == (EARLIEST, None)
    await migrator.stop()

    # Averages at another interval would mix grids
    config.sparse_interval = timedelta(minutes=2)
    with pytest.raises(migrate.MigrationConflictError, match='started with sparse_interval 60s, now it is 120s'):
        await migrator.start(args())

    with pytest.raises(migrate.MigrationConflictError, match='after now'):
        await migrator.start(args(earliest=NOW + 1))


async def test_start_done(migrator: migrate.Migrator, legacy, archive, seeded):
    await migrator.start(args())
    await finish(migrator)
    with pytest.raises(migrate.MigrationConflictError, match='done'):
        await migrator.start(args())


async def test_start_save_fails(migrator: migrate.Migrator, legacy, monkeypatch: pytest.MonkeyPatch):
    # Nothing starts before the state is saved
    async def failing_save(state: MigrationState) -> None:
        raise ConnectionError('datastore down')

    monkeypatch.setattr(migrator, 'save', failing_save)
    with pytest.raises(ConnectionError):
        await migrator.start(args())
    assert (migrator.state, migrator.running) == (None, False)
    assert victoria.CV.get().legacy_end is None


async def test_seed(
    migrator: migrate.Migrator,
    config: ServiceConfig,
    datastore: FakeDatastore,
    legacy: FakeLegacy,
    seeded: list[bytes],
):
    # A day at a time back from where the legacy samples end, for dense_days
    state = new_state(dense_days=2)
    vic = victoria.CV.get()
    vic.dense_since = NOW - 3600
    async with migrate_source() as source:
        await migrator.seed(source, state)
    end = LEGACY_END
    assert seeded == [f'native {end - DAY}-{end};'.encode(), f'native {end - 2 * DAY}-{end - DAY};'.encode()]
    assert state.phase == saved(datastore).phase == 'walk'
    # Short graphs of the seeded days read the dense database
    assert vic.dense_since == end - 2 * DAY

    # Not older than the dense database keeps
    seeded.clear()
    config.dense_retention = timedelta(days=1)
    vic.dense_since = None
    async with migrate_source() as source:
        await migrator.seed(source, new_state(dense_days=3))
    assert seeded == [f'native {NOW - DAY}-{end};'.encode()]
    assert vic.dense_since is None


async def test_seed_resumes(
    migrator: migrate.Migrator,
    datastore: FakeDatastore,
    legacy,
    archive,
    httpx_mock: HTTPXMock,
    dense_url: str,
):
    # A failure halfway through the seed: it goes on from the day that failed
    seeded: list[bytes] = []

    async def native_import(request: Request) -> Response:
        seeded.append(await request.aread())
        if len(seeded) == 2:
            return Response(500, text='dense down')
        return Response(204)

    httpx_mock.add_callback(
        url=f'{dense_url}/api/v1/import/native', method='POST', callback=native_import, is_reusable=True
    )
    await migrator.start(args(dense_days=2))
    await finish(migrator)
    end = LEGACY_END
    assert seeded == [f'native {end - DAY}-{end};'.encode()] + [f'native {end - 2 * DAY}-{end - DAY};'.encode()] * 2
    assert saved(datastore).phase == 'done'


async def test_legacy_grew(
    migrator: migrate.Migrator,
    datastore: FakeDatastore,
    legacy: FakeLegacy,
    archive,
    seeded,
):
    # After a rollback, history wrote to the legacy database again: the migration stops, and says why
    await datastore.set(state_doc(new_state(phase='walk')))
    legacy.last = NOW - 10
    status = await migrator.start(args())
    assert status.running
    await finish(migrator)
    status = migrator.status()
    assert status is not None
    assert (status.phase, status.running) == ('walk', False)
    assert status.last_error == (
        'The legacy database has samples after the last one the migration found: discard it to start again'
    )
    assert archive.events == []


async def test_legacy_check_retried(
    migrator: migrate.Migrator,
    datastore: FakeDatastore,
    legacy: FakeLegacy,
    archive: FakeArchive,
    seeded,
):
    # The legacy database does not answer yet when the migration resumes: it is checked again
    await datastore.set(state_doc(new_state(phase='walk')))
    legacy.last = NOW - 10
    legacy.failures = 1
    await migrator.start(args())
    await finish(migrator)
    status = migrator.status()
    assert status is not None
    assert status.last_error is not None
    assert status.last_error.startswith('The legacy database has samples after the last one')
    assert archive.events == []


@pytest.mark.parametrize(
    ('found', 'last', 'grew'),
    [
        # The last sample the migration found, on the grid or less than a second before it
        (LEGACY_END, LEGACY_END, False),
        (LEGACY_END - 0.5, LEGACY_END - 0.5, False),
        (NOW - 125.5, NOW - 125.5, False),
        # A newer one inside the last interval
        (NOW - 125.5, NOW - 121, True),
        (LEGACY_END - 0.5, LEGACY_END - 0.25, True),
        # A source that had none
        (None, None, False),
        (None, EARLIEST + 10, True),
    ],
)
async def test_check_legacy_end(
    migrator: migrate.Migrator,
    legacy: FakeLegacy,
    found: float | None,
    last: float | None,
    grew: bool,
):
    legacy.last = last
    state = new_state(legacy_last=found)
    async with migrate_source() as source:
        if grew:
            with pytest.raises(migrate.MigrationConflictError, match='samples after the last one'):
                await migrator.check_legacy_end(source, state)
        else:
            await migrator.check_legacy_end(source, state)


async def test_status_after_restart(migrator: migrate.Migrator, datastore: FakeDatastore):
    # Not counted until the job counts: all but the lost chunks once done
    await datastore.set(state_doc(new_state(phase='walk', cancelled=True)))
    await migrator.resume()
    status = migrator.status()
    assert status is not None
    assert status.chunks_done is None

    await datastore.set(state_doc(new_state(phase='done', lost_chunks=[NOW - 3720])))
    await migrator.resume()
    status = migrator.status()
    assert status is not None
    assert status.chunks_done == 2


async def test_missing_waits(
    migrator: migrate.Migrator,
    archive: FakeArchive,
    monkeypatch: pytest.MonkeyPatch,
    mocker: MockerFixture,
):
    # The markers imported last become searchable a few seconds later
    monkeypatch.setattr(migrate, 'SEARCHABLE_DELAY', timedelta(milliseconds=5))
    m_sleep = mocker.patch(TESTED + '.asyncio.sleep', new_callable=mocker.AsyncMock)
    assert await migrator.missing_chunks(new_state()) == ENDS
    m_sleep.assert_awaited_once_with(0.005)


async def test_seed_failure(migrator: migrate.Migrator, httpx_mock: HTTPXMock):
    httpx_mock.add_response(
        url=re.compile(re.escape(LEGACY) + r'/api/v1/export/native\?.*'),
        method='GET',
        status_code=500,
        text='oops',
    )
    async with migrate_source() as source:
        with pytest.raises(httpx.HTTPStatusError, match='500'):
            await migrator.seed(source, new_state())


async def test_seed_read_error(migrator: migrate.Migrator, legacy: FakeLegacy, seeded: list[bytes]):
    # A legacy export that breaks off names the legacy database, not the dense one that was reading it
    legacy.broken = True
    async with migrate_source() as source:
        with pytest.raises(ConnectionError, match=f'^{LEGACY}/: ReadError'):
            await migrator.seed(source, new_state())


async def test_seed_save_fails(
    migrator: migrate.Migrator,
    datastore: FakeDatastore,
    legacy,
    archive,
    seeded: list[bytes],
    monkeypatch: pytest.MonkeyPatch,
):
    # The seed is done, but saving that fails: it is saved again, without seeding again
    save = migrator.save
    failed: list[str] = []

    async def failing_save(state: MigrationState) -> None:
        if state.phase == 'walk' and not failed:
            failed.append(state.phase)
            raise ConnectionError('datastore down')
        await save(state)

    monkeypatch.setattr(migrator, 'save', failing_save)
    await migrator.start(args())
    await finish(migrator)
    assert failed == ['walk']
    assert seeded == [f'native {LEGACY_END - DAY}-{LEGACY_END};'.encode()]
    assert saved(datastore).phase == 'done'


async def test_done_save_fails(
    migrator: migrate.Migrator,
    datastore: FakeDatastore,
    legacy,
    archive: FakeArchive,
    seeded,
    monkeypatch: pytest.MonkeyPatch,
):
    # Done, but saving that fails: not done until it is saved, so the walk goes on and saves it again
    save = migrator.save
    failed: list[str] = []
    phases: list[str] = []

    async def failing_save(state: MigrationState) -> None:
        if state.phase == 'done' and not failed:
            failed.append(state.phase)
            current = migrator.state
            assert current is not None
            phases.append(current.phase)
            raise ConnectionError('datastore down')
        await save(state)

    monkeypatch.setattr(migrator, 'save', failing_save)
    await migrator.start(args())
    await finish(migrator)
    assert (failed, phases) == (['done'], ['walk'])
    assert (saved(datastore).phase, migrator.last_error) == ('done', None)
    # Every chunk averaged once
    assert len(archive.events) == 3


async def test_walk_failure(
    migrator: migrate.Migrator,
    datastore: FakeDatastore,
    legacy,
    archive: FakeArchive,
    seeded,
    caplog: pytest.LogCaptureFixture,
):
    # A failed import is tried again after a pause; the error is in the status meanwhile
    archive.failures = 1
    await migrator.start(args())
    for _ in range(100):
        if migrator.last_error is not None:
            break
        await asyncio.sleep(0.001)
    assert migrator.last_error is not None
    assert migrator.last_error.startswith('HTTPStatusError')
    await finish(migrator)
    assert (saved(datastore).phase, migrator.last_error) == ('done', None)
    assert archive.marked == sorted(ENDS)
    assert 'Migration failed (walk), trying again: HTTPStatusError' in caplog.text


async def test_walk_crash(
    migrator: migrate.Migrator,
    legacy,
    archive: FakeArchive,
    caplog: pytest.LogCaptureFixture,
):
    # The long-term database crashes and loses an import it held in memory, marker and all:
    # its chunk is averaged again
    archive.lose_imports = 1
    state = new_state(phase='walk')
    async with migrate_source() as source:
        await migrator.walk(source, state)
    assert (state.phase, state.lost_chunks) == ('done', [])
    assert archive.marked == sorted(ENDS)
    assert [e[1][-1][1] for e in archive.events] == [*ENDS, NOW - 120]
    assert 'lost 1 chunks of the migration: averaging again' in caplog.text


async def test_walk_lost(
    migrator: migrate.Migrator,
    legacy: FakeLegacy,
    archive: FakeArchive,
    caplog: pytest.LogCaptureFixture,
):
    # Chunks the long-term database does not keep (older than its retention) are lost after MAX_ATTEMPTS.
    # So are legacy series without averages.
    archive.retention_start = NOW - 7200
    legacy.names = {'a', 'b', 'c'}
    state = new_state(phase='walk')
    async with migrate_source() as source:
        await migrator.walk(source, state)
    assert (state.phase, state.lost_chunks, state.missing_series) == ('done', [NOW - 7320], [])
    assert [e[1][0][1] for e in archive.events if e[0] == 'import' and e[1][0][0] == 'a'] == [
        NOW - 120,
        NOW - 3720,
        NOW - 7320,
        NOW - 7320,
    ]
    assert '1 chunks of the migration are lost' in caplog.text

    # A legacy series without averages
    archive.lines = [line for line in archive.lines if line['metric']['__name__'] != 'c']
    state = new_state(phase='walk', earliest=NOW - 7260)
    async with migrate_source() as source:
        await migrator.walk(source, state)
    assert (state.lost_chunks, state.missing_series) == ([], ['c'])


async def test_walk_unreadable(
    migrator: migrate.Migrator,
    datastore: FakeDatastore,
    legacy: FakeLegacy,
    archive: FakeArchive,
    seeded,
    httpx_mock: HTTPXMock,
):
    # A chunk that keeps failing is tried MAX_ATTEMPTS times, and lost: the walk goes on past it
    legacy.unreadable = {NOW - 3720}
    await migrator.start(args())
    await finish(migrator)
    state = saved(datastore)
    assert (state.phase, state.lost_chunks) == ('done', [NOW - 3720])
    assert archive.marked == [NOW - 7320, NOW - 120]
    queried = [
        parse_qs(r.read().decode())['end'][0] for r in httpx_mock.get_requests(url=LEGACY + '/api/v1/query_range')
    ]
    assert queried.count(str(NOW - 3720)) == migrate.MAX_ATTEMPTS
    assert queried.count(str(NOW - 120)) == 1


async def test_walk_empty(migrator: migrate.Migrator, legacy: FakeLegacy, archive: FakeArchive):
    # Chunks without legacy samples: only their markers
    legacy.names = set()
    state = new_state(phase='walk')
    async with migrate_source() as source:
        await migrator.walk(source, state)
    assert archive.events == [('import', [(victoria.MIGRATION_MARKER, e)]) for e in ENDS]
    assert (state.phase, state.lost_chunks) == ('done', [])


async def test_pause(
    migrator: migrate.Migrator,
    clock: list[int],
    legacy: FakeLegacy,
    archive,
    mocker: MockerFixture,
    monkeypatch: pytest.MonkeyPatch,
):
    # After a chunk, a pause as long as it took; not counting a wait for a chunk that ends after now
    monkeypatch.setattr(migrate, 'PAUSE_FACTOR', 1)
    monotonic = [100.0]
    mocker.patch(TESTED + '.time.monotonic', side_effect=lambda: monotonic[0])

    async def fake_sleep(delay: float) -> None:
        monotonic[0] += delay

    m_sleep = mocker.patch(TESTED + '.asyncio.sleep', side_effect=fake_sleep)
    legacy.on_query = lambda: monotonic.__setitem__(0, monotonic[0] + 2)
    clock[0] = NOW - 130
    async with migrate_source() as source:
        await migrator.average_chunk(source, new_state(), LEGACY_END)
    assert [c.args[0] for c in m_sleep.await_args_list] == [pytest.approx(10.001), pytest.approx(2)]


async def test_cancel(migrator: migrate.Migrator, datastore: FakeDatastore, legacy, archive, seeded):
    assert await migrator.cancel() is None

    await migrator.start(args())
    status = await migrator.cancel()
    assert status is not None
    assert (status.cancelled, status.running) == (True, False)
    assert saved(datastore).cancelled

    # Not resumed at startup until started again
    await migrator.resume()
    assert not migrator.running
    status = await migrator.start(args())
    assert not status.cancelled
    await finish(migrator)
    assert saved(datastore).phase == 'done'

    # A migration that is done is not cancelled
    status = await migrator.cancel()
    assert status is not None
    assert (status.phase, status.cancelled) == ('done', False)

    # Discarded: the state is gone, and so is where legacy samples end
    assert await migrator.cancel(discard=True) is None
    assert datastore.docs == {}
    vic = victoria.CV.get()
    assert (vic.legacy_end, vic.legacy_end_known) == (None, True)


@pytest.mark.parametrize('discard', [False, True])
async def test_cancel_keeps_resume(migrator: migrate.Migrator, datastore: FakeDatastore, discard: bool):
    # A cancel while the datastore is down at startup fails, but the resume still reads the state:
    # the downsampler learns where the legacy samples end
    await datastore.set(state_doc(new_state(cancelled=True)))
    datastore.failures = 1000
    migrator.start_resume()
    await asyncio.sleep(0.01)
    with pytest.raises(ConnectionError):
        await migrator.cancel(discard=discard)
    datastore.failures = 0
    resume = migrator._resume_task
    assert resume is not None
    await asyncio.wait_for(resume, 1)
    vic = victoria.CV.get()
    assert (vic.legacy_end, vic.legacy_end_known) == (LEGACY_END, True)
    assert not migrator.running


async def test_cancel_racing_start(migrator: migrate.Migrator, datastore: FakeDatastore, legacy, archive, seeded):
    # A cancel while the start is planning: it waits for the start, then stops the job
    await asyncio.gather(migrator.start(args()), migrator.cancel())
    assert not migrator.running
    assert saved(datastore).cancelled


async def test_cancel_reads_state(migrator: migrate.Migrator, datastore: FakeDatastore):
    # Before the state was read at startup: it is read first
    await datastore.set(state_doc(new_state()))
    status = await migrator.cancel()
    assert status is not None
    assert status.cancelled
    assert saved(datastore).cancelled


async def test_resume(
    migrator: migrate.Migrator,
    datastore: FakeDatastore,
    legacy,
    archive: FakeArchive,
    seeded,
    caplog: pytest.LogCaptureFixture,
):
    # A restart halfway through the walk: the job's markers tell which chunks are left.
    # Another job's do not: it was discarded.
    await datastore.set(state_doc(new_state(phase='walk')))
    marker = victoria.MIGRATION_MARKER
    archive.lines = [
        {'metric': {'__name__': marker, 'migration': JOB}, 'values': [1], 'timestamps': [LEGACY_END * 1000]},
        {'metric': {'__name__': marker, 'migration': 'old'}, 'values': [1], 'timestamps': [(NOW - 3720) * 1000]},
    ]
    datastore.failures = 1
    counted: list[int | None] = []
    archive.on_import = lambda: counted.append(migrator.chunks_done)
    await migrator.resume()
    assert 'Migration state not read, trying again' in caplog.text
    await finish(migrator)
    assert saved(datastore).phase == 'done'
    assert [e[1][-1] for e in archive.events] == [(marker, NOW - 3720), (marker, NOW - 7320)]
    # The markers found count, and each chunk averaged since
    assert counted == [1, 2]


async def test_resume_racing_start(
    migrator: migrate.Migrator,
    datastore: FakeDatastore,
    legacy,
    archive: FakeArchive,
    seeded,
):
    # The datastore answers late at startup, and a request starts the migration first:
    # the resume leaves it be, and the job keeps its state
    datastore.failures = 1
    migrator.start_resume()
    await asyncio.sleep(0)
    status = await migrator.start(args())
    assert status.running
    state = migrator.state
    assert state is not None
    await finish(migrator)
    resume = migrator._resume_task
    assert resume is not None
    await asyncio.wait_for(resume, 1)
    assert migrator.state is state
    assert (state.phase, saved(datastore).phase) == ('done', 'done')


async def test_stop_resume(migrator: migrate.Migrator, datastore: FakeDatastore):
    # Stopping the service also stops a resume waiting for the datastore
    datastore.failures = 100
    migrator.start_resume()
    task = migrator._resume_task
    assert task is not None
    await asyncio.sleep(0.01)
    await migrator.stop()
    assert task.cancelled()
    assert migrator._resume_task is None


async def test_resume_refused(
    migrator: migrate.Migrator,
    config: ServiceConfig,
    datastore: FakeDatastore,
    caplog: pytest.LogCaptureFixture,
):
    # Not at another interval: why is in the status
    await datastore.set(state_doc(new_state()))
    config.sparse_interval = timedelta(minutes=2)
    await migrator.resume()
    status = migrator.status()
    assert status is not None
    assert (status.running, status.last_error) == (
        False,
        'The migration started with sparse_interval 60s, now it is 120s',
    )

    # A state that is not valid is not read again and again, and cannot be started or cancelled: only discarded.
    # The downsampler still learns where the legacy samples end.
    datastore.docs[(migrate.NAMESPACE, migrate.DOC_ID)] = json.dumps(
        {'namespace': migrate.NAMESPACE, 'id': migrate.DOC_ID, 'legacy_end': LEGACY_END}
    )
    victoria.setup()
    migrate.setup()
    migrator = migrate.CV.get()
    await asyncio.wait_for(migrator.resume(), 1)
    assert 'The migration state is not valid, and not resumed' in caplog.text
    vic = victoria.CV.get()
    assert (vic.legacy_end, vic.legacy_end_known) == (LEGACY_END, True)
    config.sparse_interval = timedelta(minutes=1)
    with pytest.raises(migrate.MigrationConflictError, match='not valid, discard it'):
        await migrator.start(args())
    with pytest.raises(migrate.MigrationConflictError, match='not valid, discard it'):
        await migrator.cancel()
    assert await migrator.cancel(discard=True) is None

    # Not a datastore document at all: where the legacy samples end is not known, and not waited for
    datastore.docs[(migrate.NAMESPACE, migrate.DOC_ID)] = 'not json'
    victoria.setup()
    migrate.setup()
    migrator = migrate.CV.get()
    await asyncio.wait_for(migrator.resume(), 1)
    vic = victoria.CV.get()
    assert (vic.legacy_end, vic.legacy_end_known) == (None, True)
    with pytest.raises(migrate.MigrationConflictError, match='not valid, discard it'):
        await migrator.start(args())
    assert await migrator.cancel(discard=True) is None
    assert migrator.status() is None


async def test_lifespan(
    migrator: migrate.Migrator,
    config: ServiceConfig,
    datastore: FakeDatastore,
    legacy,
    archive,
    seeded,
):
    # No migration: nothing to resume
    async with migrate.lifespan():
        assert victoria.CV.get().legacy_end_known
    assert migrator.state is None

    # A migration that is not done resumes
    await datastore.set(state_doc(new_state()))
    async with migrate.lifespan():
        assert victoria.CV.get().legacy_end == LEGACY_END
        for _ in range(100):
            if migrator.state is not None and migrator.state.phase == 'done':
                break
            await asyncio.sleep(0.01)
    assert saved(datastore).phase == 'done'


@pytest.fixture
def m_migrator(mocker: MockerFixture) -> Mock:
    m = mocker.patch(TESTED + '.CV').get.return_value
    m.start = mocker.AsyncMock()
    m.cancel = mocker.AsyncMock(return_value=None)
    m.status = Mock(return_value=None)
    return m


@pytest.fixture
def app(config: ServiceConfig, m_migrator: Mock) -> FastAPI:
    downsample.setup()
    app = FastAPI()
    app.include_router(timeseries_api.router)
    app_factory.add_exception_handlers(app)
    return app


async def test_api(app: FastAPI, m_migrator: Mock):
    m = m_migrator
    status = MigrationStatus(**new_state(phase='walk').model_dump(), running=True, chunks_done=1, last_error=None)
    m.start.return_value = status

    async with LifespanManager(app), AsyncClient(base_url='http://test', transport=ASGITransport(app=app)) as client:
        resp = await client.get('/timeseries/migrate')
        assert (resp.status_code, resp.json()) == (200, None)

        body = {'source_url': LEGACY, 'earliest': '2021-07-01T00:00:00Z'}
        resp = await client.post('/timeseries/migrate', json=body)
        assert (resp.status_code, resp.json()['phase'], resp.json()['chunks_done']) == (200, 'walk', 1)
        assert m.start.await_args.args[0] == MigrationArgs(source_url=LEGACY, earliest=at(1625097600), dense_days=30)

        m.start.side_effect = migrate.MigrationConflictError('The migration is running')
        resp = await client.post('/timeseries/migrate', json=body)
        assert (resp.status_code, resp.json()) == (409, {'detail': 'The migration is running'})

        resp = await client.delete('/timeseries/migrate')
        assert (resp.status_code, resp.json()) == (200, None)
        m.cancel.assert_awaited_with(discard=False)
        await client.delete('/timeseries/migrate', params={'discard': True})
        m.cancel.assert_awaited_with(discard=True)
        m.cancel.side_effect = migrate.MigrationConflictError('The migration state is not valid, discard it')
        resp = await client.delete('/timeseries/migrate')
        assert resp.status_code == 409


async def test_downsampler_after_legacy(
    migrator: migrate.Migrator,
    httpx_mock: HTTPXMock,
    url: str,
    caplog: pytest.LogCaptureFixture,
):
    # Without averages in the long-term database, the downsampler waits for the migration state,
    # then starts where the legacy samples end
    caplog.set_level(logging.INFO)
    downsample.setup()
    ds = downsample.CV.get()
    vic = victoria.CV.get()
    httpx_mock.add_callback(url=f'{url}/api/v1/query', method='POST', callback=lambda _: vector(None), is_reusable=True)
    httpx_mock.add_response(url=f'{url}/api/v1/series', method='POST', json={'status': 'success', 'data': []})
    assert await ds.discover_cursor(NOW, NOW - DAY) is None
    assert 'Waiting for the migration state' in caplog.text

    vic.legacy_end = NOW - 90
    vic.legacy_end_known = True
    assert await ds.discover_cursor(NOW, NOW - DAY) == NOW - 60


def dense_averages(request: Request) -> Response:
    """The dense database's averages: series a, 1 at every point."""
    p = params(request)
    start, end, step = int(p['start']), int(p['end']), int(p['step'].rstrip('s'))
    result = [{'metric': {'__name__': 'a'}, 'values': [[t, '1'] for t in range(start, end + 1, step)]}]
    return Response(200, json={'status': 'success', 'data': {'resultType': 'matrix', 'result': result}})


async def test_downsampler_fence(migrator: migrate.Migrator, httpx_mock: HTTPXMock, url: str, dense_url: str):
    # A migration is planned while the downsampler reads a chunk: that chunk is not imported,
    # and the downsampler goes on from where the legacy samples end
    downsample.setup()
    ds = downsample.CV.get()
    vic = victoria.CV.get()

    def planned(request: Request) -> Response:
        vic.legacy_end = NOW - 90
        return dense_averages(request)

    httpx_mock.add_callback(url=f'{dense_url}/api/v1/query_range', method='POST', callback=planned, is_reusable=True)
    httpx_mock.add_response(url=f'{url}/api/v1/import', method='POST', status_code=204)
    ds.cursor = NOW - 600
    await ds.downsample(NOW + 30)
    assert ds.cursor == NOW
    [request] = httpx_mock.get_requests(url=f'{url}/api/v1/import')
    lines = [json.loads(line) for line in request.read().decode().splitlines()]
    assert [(line['metric']['__name__'], line['timestamps']) for line in lines] == [
        ('a', [NOW * 1000]),
        (victoria.MARKER, [NOW * 1000]),
    ]


async def test_plan_waits_for_import(migrator: migrate.Migrator, httpx_mock: HTTPXMock, url: str, dense_url: str):
    # An import under way when a migration is planned is done first:
    # the downsampler imports nothing before a legacy_end it has not seen
    downsample.setup()
    ds = downsample.CV.get()
    importing = asyncio.Event()
    release = asyncio.Event()

    async def slow_import(request: Request) -> Response:
        importing.set()
        await release.wait()
        return Response(204)

    httpx_mock.add_callback(url=f'{dense_url}/api/v1/query_range', method='POST', callback=dense_averages)
    httpx_mock.add_callback(url=f'{url}/api/v1/import', method='POST', callback=slow_import)
    ds.cursor = NOW - 60
    downsampling = asyncio.create_task(ds.downsample(NOW + 30))
    await asyncio.wait_for(importing.wait(), 1)
    planning = asyncio.create_task(migrator._set_legacy_end(NOW - 30))
    await asyncio.sleep(0.01)
    assert not planning.done()
    release.set()
    await asyncio.wait_for(asyncio.gather(downsampling, planning), 1)
    assert victoria.CV.get().legacy_end == NOW - 30
