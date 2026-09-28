"""
Tests brewblox_history.victoria against the real databases of test/docker-compose.yml.

Series names are unique per test: the databases live for the whole test session.
"""

import asyncio
import itertools
import json
from datetime import timedelta

import httpx
import pytest
from pytest_mock import MockerFixture

from brewblox_history import downsample, migrate, planner, redis, utils, victoria
from brewblox_history.models import (
    HistoryEvent,
    MigrationArgs,
    MigrationState,
    ServiceConfig,
    TimeSeriesCsvQuery,
    TimeSeriesFieldsQuery,
    TimeSeriesRangesQuery,
)
from test.conftest import FakeDatastore

# Names the escaping on the way in and out must keep intact
NAMES = ['plain', 'a "quoted" (x)', 'back\\slash\\', 'temp[degC]', "tab\tit's °"]


@pytest.fixture
def now(monkeypatch: pytest.MonkeyPatch) -> int:
    """The clock stops: test data and queries are planned from the same moment.
    The databases keep the real time, which only runs ahead of it."""
    frozen = utils.now()
    monkeypatch.setattr(utils, 'now', lambda: frozen)
    return int(frozen.timestamp())


@pytest.fixture
def db(config: ServiceConfig) -> victoria.VictoriaClient:
    victoria.setup()
    return victoria.CV.get()


async def import_samples(client: httpx.AsyncClient, series: dict[str, list[tuple[int, float]]]):
    """Writes samples at the given timestamps (ms)."""
    lines = [
        json.dumps(
            {'metric': {'__name__': name}, 'values': [v for _, v in samples], 'timestamps': [t for t, _ in samples]}
        )
        for name, samples in series.items()
    ]
    (await client.post('/api/v1/import', content='\n'.join(lines))).raise_for_status()


async def exported(client: httpx.AsyncClient, prefix: str, end: int | None = None) -> dict[str, dict]:
    """Each series' samples, by name, up to end (default: now).
    The database may export a series in several lines: they are merged."""
    params = {'match[]': f'{{__name__=~"{prefix}.*"}}'} | ({} if end is None else {'end': str(end)})
    resp = await client.post('/api/v1/export', data=params)
    resp.raise_for_status()
    samples: dict[str, list[tuple[int, float]]] = {}
    for row in map(json.loads, resp.text.splitlines()):
        samples.setdefault(row['metric']['__name__'], []).extend(zip(row['timestamps'], row['values'], strict=True))
    return {
        name: {'timestamps': [t for t, _ in sorted(pairs)], 'values': [v for _, v in sorted(pairs)]}
        for name, pairs in samples.items()
    }


async def wait_searchable(client: httpx.AsyncClient, prefix: str, series: dict[str, list]):
    """New samples become searchable about a second after a forced flush."""
    for _ in range(50):
        (await client.get('/internal/force_flush')).raise_for_status()
        rows = await exported(client, prefix)
        if all(len(rows.get(name, {}).get('timestamps', [])) == len(samples) for name, samples in series.items()):
            return
        await asyncio.sleep(0.1)
    raise TimeoutError(f'{prefix} not searchable')


async def test_write_database(config: ServiceConfig):
    # Names as stored in the dense database, and timestamps
    victoria.setup()
    vic = victoria.CV.get()
    dense = vic._database('dense')
    key = 'itest "spark",1\\'
    data = {'block, one': {'value=x': 1}, 'back\\slash\\': 2, 'temp[°C]': 3}
    timestamp = utils.to_millis(utils.now()) - 2000

    await vic.write(HistoryEvent(key=key, data=data, timestamp=timestamp))
    await vic.write(HistoryEvent(key=key, data={'arrival': 4}))

    stamped = {f'{key}/block, one/value=x', f'{key}/back\\slash\\', f'{key}/temp[°C]'}
    expected = stamped | {f'{key}/arrival'}
    rows = {}
    for _ in range(50):  # New samples become searchable about a second after a forced flush
        (await dense.get('/internal/force_flush')).raise_for_status()
        rows = await exported(dense, 'itest')
        if expected <= rows.keys():
            break
        await asyncio.sleep(0.1)

    assert rows.keys() == expected
    for name in stamped:
        assert rows[name]['timestamps'] == [timestamp]
    assert rows[f'{key}/arrival']['timestamps'][0] > timestamp


async def test_ranges_seam(db: victoria.VictoriaClient, now: int):
    # A day: averages from the long-term database up to the cursor, raw samples after it
    names = [f'seam/{n}' for n in NAMES]
    minute = now - now % 60
    averages = {n: [(t * 1000, 1.0) for t in range(minute - 26 * 3600, minute - 600 + 1, 60)] for n in names}
    raw = {n: [((t + 5) * 1000, 2.0) for t in range(now - 1800, now - 10, 10)] for n in names}
    await import_samples(db._archive, averages)
    await import_samples(db._database('dense'), raw)
    await wait_searchable(db._archive, 'seam', averages)
    await wait_searchable(db._database('dense'), 'seam', raw)

    db.cursor = minute - 1200
    result = await db.ranges(TimeSeriesRangesQuery(fields=names, duration=timedelta(days=1)))
    assert [r.metric.name for r in result] == names

    # 1d gives 86 s, rounded up to a multiple of the 60 s averages
    step = 120
    seam = db.cursor - db.cursor % step
    for r in result:
        timestamps = [v.timestamp for v in r.values]
        assert all(b - a == step for a, b in itertools.pairwise(timestamps))
        assert timestamps[0] <= now - 24 * 3600
        assert seam in timestamps
        assert timestamps[-1] > seam
        assert timestamps[-1] <= now - 5
        assert {v.value for v in r.values if v.timestamp <= seam} == {'1'}
        assert {v.value for v in r.values if v.timestamp > seam} == {'2'}


async def test_ranges_dense(db: victoria.VictoriaClient, config: ServiceConfig, now: int):
    # Ten minutes: raw samples, at the requested step
    config.minimum_step = timedelta(seconds=10)
    names = [f'dense/{n}' for n in NAMES[:2]]
    # A sample every second, in the middle of it, valued by its second
    raw = {n: [(t * 1000 + 500, float(t)) for t in range(now - 900, now - 1)] for n in names}
    await import_samples(db._database('dense'), raw)
    await wait_searchable(db._database('dense'), 'dense', raw)

    result = await db.ranges(TimeSeriesRangesQuery(fields=names, duration=timedelta(minutes=10)))
    assert [r.metric.name for r in result] == names
    for r in result:
        # Each point averages the ten samples of the step before it.
        # A point the database replaced with an older one (latency offset) would not match.
        assert [float(v.value) for v in r.values] == [v.timestamp - 5.5 for v in r.values]
        assert r.values[-1].timestamp >= now - 5 - 10


async def test_follow_up_database(db: victoria.VictoriaClient, now: int, monkeypatch: pytest.MonkeyPatch):
    # A live stream: the follow-ups continue exactly after the initial points, with real averages
    names = [f'follow/{n}' for n in NAMES[:2]]
    # A sample every second, in the middle of it, valued by its second
    raw = {n: [(t * 1000 + 500, float(t)) for t in range(now - 900, now - 1)] for n in names}
    await import_samples(db._database('dense'), raw)
    await wait_searchable(db._database('dense'), 'follow', raw)

    # Ten minutes at 1 s, three seconds ago: up to query_latency (5 s) before then.
    # The clock only goes back: the database would replace points it thinks are in the last 5 s.
    frozen = utils.now()
    monkeypatch.setattr(utils, 'now', lambda: frozen - timedelta(seconds=3))
    _, follow = await db.initial_ranges(TimeSeriesRangesQuery(fields=names, duration=timedelta(minutes=10)))
    assert follow == (now - 8, 1, now - 8)

    # Now: the three points after it. A point the database replaced with an older one would not match.
    monkeypatch.setattr(utils, 'now', lambda: frozen)
    result, follow = await db.follow_up_ranges(names, follow)
    assert [r.metric.name for r in result] == names
    assert follow == (now - 5, 1, now - 5)
    for r in result:
        assert r.values == [(t, str(t - 1)) for t in range(now - 7, now - 4)]

    # A follow-up at 10 s after a point off that grid, of over 50 points: the database keeps its start
    last = now - 700 - (now - 700) % 10 + 7
    result, follow = await db.follow_up_ranges(names, planner.FollowUp(last, 10, last))
    points = list(range(last + 10, now - 5 + 1, 10))
    assert len(points) >= 50
    assert [r.metric.name for r in result] == names
    assert follow == (points[-1], 10, points[-1])
    for r in result:
        # Each point averages the ten samples of the step before it
        assert [v.timestamp for v in r.values] == points
        assert [float(v.value) for v in r.values] == [t - 5.5 for t in points]


async def test_ranges_fallback(db: victoria.VictoriaClient, now: int):
    # The dense database has none of the fields: the long-term database answers
    names = ['fallback/x']
    minute = now - now % 60
    averages = {n: [(t * 1000, 7.0) for t in range(minute - 3600, minute + 1, 60)] for n in names}
    await import_samples(db._archive, averages)
    await wait_searchable(db._archive, 'fallback', averages)

    [r] = await db.ranges(TimeSeriesRangesQuery(fields=names, duration=timedelta(minutes=10)))
    timestamps = [v.timestamp for v in r.values]
    assert all(b - a == 60 for a, b in itertools.pairwise(timestamps))
    assert {v.value for v in r.values} == {'7'}


async def test_ranges_refined_database(db: victoria.VictoriaClient, now: int, caplog: pytest.LogCaptureFixture):
    # Two years with little in them: asked again, finer, where the samples are, and never refused
    # (the database refuses queries for more than 30000 points per series)
    minute = now - now % 60
    db.cursor = minute - 600
    query_ranges = db._query_ranges
    # The step of the plans asked (whether one has a dense part after the cursor depends on the clock),
    # and how many points they asked for per series
    asked: list[set[int]] = []
    points: list[int] = []

    async def spy(queries: list[planner.RangeQuery], names: list[str]) -> tuple[dict[str, list], bool]:
        asked.append({q.step for q in queries})
        points.append(sum((q.end - q.start) // q.step + 1 for q in queries))
        return await query_ranges(queries, names)

    db._query_ranges = spy  # type: ignore[method-assign]

    # Three days of averages a year ago, from a minute into a step of the first query (63120 s, on the epoch's
    # grid): its answer has 5 points, wherever the clock is
    names = [f'refined/{n}' for n in NAMES[:2]]
    since = minute - 365 * 86400
    since -= since % 63120 - 60
    averages = {n: [(t * 1000, 1.0) for t in range(since, since + 3 * 86400, 60)] for n in names}
    await import_samples(db._archive, averages)
    await wait_searchable(db._archive, 'refined', averages)

    result = await db.ranges(TimeSeriesRangesQuery(fields=names, duration=timedelta(days=730)))
    assert asked == [{63120}, {360}]
    for r in result:
        timestamps = [v.timestamp for v in r.values]
        assert timestamps == list(range(int(timestamps[0]), int(timestamps[-1]) + 1, 360))
        assert len(timestamps) >= 720
        assert {v.value for v in r.values} == {'1'}

    # A day at either end: the step spreads at most REFINE_MAX_POINTS over two years
    names = [f'ends/{n}' for n in NAMES[:2]]
    ends = [*range(minute - 730 * 86400, minute - 729 * 86400, 60), *range(minute - 86400, minute - 600 + 1, 60)]
    averages = {n: [(t * 1000, 2.0) for t in ends] for n in names}
    await import_samples(db._archive, averages)
    await wait_searchable(db._archive, 'ends', averages)

    asked.clear()
    points.clear()
    result = await db.ranges(TimeSeriesRangesQuery(fields=names, duration=timedelta(days=730)))
    assert asked == [{63120}, {2580}]
    assert 24000 < points[1] <= planner.REFINE_MAX_POINTS
    for r in result:
        timestamps = [v.timestamp for v in r.values]
        assert all((b - a) % 2580 == 0 for a, b in itertools.pairwise(timestamps))
        assert len(timestamps) > 60
    assert 'the finer query failed' not in caplog.text


async def test_csv_seam(db: victoria.VictoriaClient, config: ServiceConfig, now: int):
    # Raw samples where the dense database has them, averages before that; exported in windows
    config.dense_retention = timedelta(hours=2)
    config.dense_margin = timedelta()
    config.csv_chunk_dense = timedelta(minutes=30)

    names = [f'csv/{n}' for n in ['a', 'b "c"', 'back\\slash']]
    minute = now - now % 60
    averages = {n: [(t * 1000, 1.0) for t in range(minute - 3 * 3600, minute - 3600 + 1, 60)] for n in names}
    # Raw samples 5 s past every 10 s of the minute grid
    raw = {n: [((t + 5) * 1000, 2.0) for t in range(minute - 3 * 3600, now - 10, 10)] for n in names}
    await import_samples(db._archive, averages)
    await import_samples(db._database('dense'), raw)
    await wait_searchable(db._archive, 'csv', averages)
    await wait_searchable(db._database('dense'), 'csv', raw)

    lines = [
        line async for line in db.csv(TimeSeriesCsvQuery(fields=names, duration=timedelta(hours=3), precision='ms'))
    ]
    assert lines[0] == ','.join(['time', *names])
    rows = [line.split(',') for line in lines[1:]]
    timestamps = [int(row[0]) for row in rows]
    values = [row[1:] for row in rows]

    # Each row once, in order, with every column
    assert timestamps == sorted(set(timestamps))
    assert all(v[0] == v[1] == v[2] for v in values)

    # Averages every minute up to the retention horizon (now - 2h), rounded up to the minute,
    # then raw samples every 10 s, starting 5 s after the last average: nothing in between is lost
    switch = [v[0] for v in values].index('2')
    horizon = now - 2 * 3600
    assert {v[0] for v in values[:switch]} == {'1'}
    assert {v[0] for v in values[switch:]} == {'2'}
    assert all(b - a == 60_000 for a, b in itertools.pairwise(timestamps[:switch]))
    assert all(b - a == 10_000 for a, b in itertools.pairwise(timestamps[switch:]))
    assert timestamps[switch - 1] == (horizon + (-horizon % 60)) * 1000
    assert timestamps[switch] == timestamps[switch - 1] + 5_000


async def test_downsample_database(db: victoria.VictoriaClient, now: int, monkeypatch: pytest.MonkeyPatch):
    # Averages in the long-term database equal the raw samples' averages, with names intact.
    # A new downsampler finds where they end, and averaging the same time again changes nothing.
    monkeypatch.setattr(downsample, 'SEARCHABLE_DELAY', timedelta(0))
    names = [f'down/{n}' for n in NAMES]
    minute = now - now % 60
    # The first passes run a minute early, the last one at now: a minute further
    target = now - 90 - (now - 90) % 60

    def value(second: int, idx: int) -> float:
        return (second % 97) * 1.25 + idx

    # A sample every second, in the middle of it (the database exports nothing after now)
    raw = {n: [(t * 1000 + 500, value(t, i)) for t in range(minute - 1800, now - 1)] for i, n in enumerate(names)}
    await import_samples(db._database('dense'), raw)
    await wait_searchable(db._database('dense'), 'down', raw)

    def expected(until: int) -> dict[str, dict[int, float]]:
        # The interval (t - 60, t] holds the samples of seconds t - 60 to t - 1
        return {
            name: {
                t * 1000: sum(value(s, i) for s in range(t - 60, t)) / 60 for t in range(minute - 1740, until + 1, 60)
            }
            for i, name in enumerate(names)
        }

    downsample.setup()
    ds = downsample.CV.get()
    ds.cursor = minute - 1800
    await ds.downsample(now - 60)
    assert ds.cursor == target
    await wait_searchable(db._archive, 'down', {n: list(p) for n, p in expected(target).items()})

    # A new downsampler finds the cursor from the marker, and averages the hour before it again
    downsample.setup()
    ds = downsample.CV.get()
    db.legacy_end_known = True
    await wait_searchable(db._archive, victoria.MARKER, {victoria.MARKER: [0]})
    ds.cursor = await ds.discover_cursor(now, None)
    assert ds.cursor == target - 3600
    await ds.downsample(now - 60)
    assert ds.cursor == target

    # Averaging from the start again, a minute later: the new minute shows the import is searchable,
    # and the minutes averaged before are unchanged
    ds.cursor = minute - 1800
    await ds.downsample(now)
    assert ds.cursor == target + 60
    points = expected(target + 60)
    await wait_searchable(db._archive, 'down', {n: list(p) for n, p in points.items()})
    rows = await exported(db._archive, 'down')
    assert rows.keys() == set(names)
    for name, series in points.items():
        assert rows[name]['timestamps'] == list(series)
        assert rows[name]['values'] == pytest.approx(list(series.values()), abs=1e-9)


async def test_write_downsample_database(db: victoria.VictoriaClient, monkeypatch: pytest.MonkeyPatch):
    # Names as the line protocol escapes them on the way into the dense database reach the long-term database
    # unchanged through the downsampler's import, and are listed once
    monkeypatch.setattr(downsample, 'SEARCHABLE_DELAY', timedelta(0))
    real = int(utils.now().timestamp())
    # Half an hour ago, so the databases replace no point: history's clock follows the samples,
    # so each one is written with its own timestamp
    minute = real - 1800 - real % 60
    clock = [minute]
    monkeypatch.setattr(utils, 'now', lambda: utils.from_millis(clock[0] * 1000))
    key = 'wdtest "spark",1\\'
    seconds = range(minute - 59, minute + 1)
    for t in seconds:
        clock[0] = t
        data = {'block, one': {'value=x': t % 7}, 'back\\slash\\': 2.5, 'temp[°C]': t / 4}
        await db.write(HistoryEvent(key=key, data=data, timestamp=t * 1000))
    averages = {
        f'{key}/block, one/value=x': sum(t % 7 for t in seconds) / 60,
        f'{key}/back\\slash\\': 2.5,
        f'{key}/temp[°C]': sum(t / 4 for t in seconds) / 60,
    }
    await wait_searchable(db._database('dense'), 'wdtest', {name: list(seconds) for name in averages})

    downsample.setup()
    ds = downsample.CV.get()
    ds.cursor = minute - 60
    await ds.downsample(minute + 60)
    assert ds.cursor == minute
    await wait_searchable(db._archive, 'wdtest', {name: [minute] for name in averages})
    rows = await exported(db._archive, 'wdtest')
    assert rows.keys() == averages.keys()
    for name, average in averages.items():
        assert rows[name] == {'timestamps': [minute * 1000], 'values': [pytest.approx(average)]}

    fields = await db.fields(TimeSeriesFieldsQuery(duration=timedelta(hours=1)))
    assert [f for f in fields if f.startswith('wdtest')] == sorted(averages)


async def test_find_dense_since_database(db: victoria.VictoriaClient, now: int):
    # The oldest sample in the dense database: other tests only write the last few hours
    downsample.setup()
    first = now - 2 * 24 * 3600 + 123
    await import_samples(db._database('dense'), {'since/a': [(first * 1000, 1.0), ((first + 3600) * 1000, 2.0)]})
    await wait_searchable(db._database('dense'), 'since', {'since/a': [0, 0]})
    assert await downsample.CV.get().find_dense_since(now) == first


async def test_has_series_database(db: victoria.VictoriaClient, now: int):
    # The whole index (start=1: the database takes 0 as not set) finds a series older than the dense retention
    old = now - 40 * 24 * 3600
    await import_samples(db._archive, {'old/a': [(old * 1000, 1.0)]})
    for _ in range(50):
        (await db._archive.get('/internal/force_flush')).raise_for_status()
        if await db.has_series('archive', 1, now, '{__name__="old/a"}'):
            break
        await asyncio.sleep(0.1)
    assert await db.has_series('archive', 1, now, '{__name__="old/a"}')
    assert not await db.has_series('archive', 1, now, '{__name__="old/none"}')
    # A day's lookup only finds series with samples that day
    assert not await db.has_series('archive', now - 24 * 3600, now, '{__name__="old/a"}')


async def test_migrate_database(
    db: victoria.VictoriaClient,
    config: ServiceConfig,
    now: int,
    legacy_url: str,
    monkeypatch: pytest.MonkeyPatch,
    mocker: MockerFixture,
):
    # A legacy database, as ctl ran it before 0.12.0 (v1.129.1): its history averaged into the long-term database,
    # its last day of raw samples copied into the dense database, with names intact
    monkeypatch.setattr(migrate, 'PAUSE_FACTOR', 0)
    monkeypatch.setattr(migrate, 'SEARCHABLE_DELAY', timedelta())
    mocker.patch.object(redis, 'CV').get.return_value = FakeDatastore()
    migrate.setup()
    migrator = migrate.CV.get()

    # Below a minute, the database replaces points newer than the latency offset: the legacy one must not.
    # The last interval ends 6-16 s before now, within the database's default offset (30 s), not after now.
    interval = 10
    config.sparse_interval = timedelta(seconds=interval)
    names = [f'migrate/{n}' for n in NAMES]
    earliest = now - now % 60 - 3 * 3600
    legacy_end = (now - 6) - (now - 6) % interval

    def value(second: int, idx: int) -> float:
        return (second % 97) * 1.25 + idx

    # Two samples an interval: 2 and 7 s past it
    seconds = range(earliest + 2, legacy_end - 2, 5)
    raw = {n: [(t * 1000, value(t, i)) for t in seconds] for i, n in enumerate(names)}

    async with httpx.AsyncClient(base_url=legacy_url) as legacy, victoria.make_client(legacy_url) as source:
        await import_samples(legacy, raw)
        await wait_searchable(legacy, 'migrate', raw)

        migrator.state = await migrator.plan(
            MigrationArgs(source_url=legacy_url, earliest=utils.from_millis(earliest * 1000), dense_days=1)
        )
        state = migrator.state
        assert (state.earliest, state.legacy_end) == (earliest - interval, legacy_end)
        ends = migrate.chunk_ends(state.legacy_end, state.earliest, state.chunk)
        assert state.chunks_total == len(ends)
        # Its last sample is not taken for a newer one
        assert state.legacy_last == seconds[-1]
        await migrator.check_legacy_end(source, state)

        await migrator.seed(source, state)

        # The walk counts markers without waiting (SEARCHABLE_DELAY is 0 here): the test database makes them
        # searchable about a second after a forced flush. Every chunk averaged has its marker.
        count = migrate.Migrator.missing_chunks

        async def searchable(self: migrate.Migrator, state: MigrationState) -> list[int]:
            for _ in range(50):
                (await db._archive.get('/internal/force_flush')).raise_for_status()
                marked = await db.archive_timestamps(migrate.marker_selector(state), state.earliest, legacy_end)
                if len(marked) >= len(self._attempts):
                    break
                await asyncio.sleep(0.1)
            return await count(self, state)

        monkeypatch.setattr(migrate.Migrator, 'missing_chunks', searchable)
        await migrator.walk(source, state)
    assert (state.phase, state.lost_chunks, state.missing_series, migrator.chunks_done) == ('done', [], [], len(ends))

    # Raw samples in the dense database, as the legacy database has them
    await wait_searchable(db._database('dense'), 'migrate', raw)
    rows = await exported(db._database('dense'), 'migrate')
    assert rows.keys() == set(names)
    for name, samples in raw.items():
        assert rows[name]['timestamps'] == [t for t, _ in samples]
        assert rows[name]['values'] == [v for _, v in samples]

    # Every interval's average in the long-term database, stamped at its end
    rows = await exported(db._archive, 'migrate', end=legacy_end)
    assert rows.keys() == set(names)
    for i, name in enumerate(names):
        expected = {}
        for end in range(state.earliest + interval, legacy_end + 1, interval):
            window = [value(t, i) for t in seconds if end - interval < t <= end]
            if window:
                expected[end * 1000] = sum(window) / len(window)
        assert rows[name]['timestamps'] == list(expected)
        assert rows[name]['values'] == pytest.approx(list(expected.values()), abs=1e-9)
