"""
Tests brewblox_history.victoria against the real databases of test/docker-compose.yml.

Series names are unique per test: the databases live for the whole test session.
"""

import asyncio
import json
from datetime import timedelta

import httpx
import pytest

from brewblox_history import utils, victoria
from brewblox_history.models import HistoryEvent, ServiceConfig, TimeSeriesCsvQuery, TimeSeriesRangesQuery

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
    config.dense_enabled = True
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


async def exported(client: httpx.AsyncClient, prefix: str) -> dict[str, dict]:
    resp = await client.post('/api/v1/export', data={'match[]': f'{{__name__=~"{prefix}.*"}}'})
    resp.raise_for_status()
    return {row['metric']['__name__']: row for row in map(json.loads, resp.text.splitlines())}


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
    # Names as stored, and timestamps
    victoria.setup()
    vic = victoria.CV.get()
    key = 'itest "spark",1\\'
    data = {'block, one': {'value=x': 1}, 'back\\slash\\': 2, 'temp[°C]': 3}
    timestamp = utils.to_millis(utils.now()) - 2000

    await vic.write(HistoryEvent(key=key, data=data, timestamp=timestamp))
    await vic.write(HistoryEvent(key=key, data={'arrival': 4}))

    stamped = {f'{key}/block, one/value=x', f'{key}/back\\slash\\', f'{key}/temp[°C]'}
    expected = stamped | {f'{key}/arrival'}
    rows = {}
    for _ in range(50):  # New samples become searchable about a second after a forced flush
        (await vic._archive.get('/internal/force_flush')).raise_for_status()
        rows = await exported(vic._archive, 'itest')
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
    await import_samples(db._dense, raw)
    await wait_searchable(db._archive, 'seam', averages)
    await wait_searchable(db._dense, 'seam', raw)

    db.cursor = minute - 1200
    result = await db.ranges(TimeSeriesRangesQuery(fields=names, duration='1d'))
    assert [r.metric.name for r in result] == names

    # 1d gives 86 s, rounded up to a multiple of the 60 s averages
    step = 120
    seam = db.cursor - db.cursor % step
    for r in result:
        timestamps = [v.timestamp for v in r.values]
        assert all(b - a == step for a, b in zip(timestamps, timestamps[1:], strict=False))
        assert timestamps[0] <= now - 24 * 3600
        assert seam in timestamps
        assert timestamps[-1] > seam
        assert timestamps[-1] <= now - 3
        assert {v.value for v in r.values if v.timestamp <= seam} == {'1'}
        assert {v.value for v in r.values if v.timestamp > seam} == {'2'}


async def test_ranges_dense(db: victoria.VictoriaClient, now: int):
    # Ten minutes: raw samples, at the requested step
    names = [f'dense/{n}' for n in NAMES[:2]]
    # A sample every second, in the middle of it, valued by its second
    raw = {n: [(t * 1000 + 500, float(t)) for t in range(now - 900, now - 1)] for n in names}
    await import_samples(db._dense, raw)
    await wait_searchable(db._dense, 'dense', raw)

    result = await db.ranges(TimeSeriesRangesQuery(fields=names, duration='10m'))
    assert [r.metric.name for r in result] == names
    for r in result:
        # Each point averages the ten samples of the step before it.
        # A point the database replaced with an older one (latency offset) would not match.
        assert [float(v.value) for v in r.values] == [v.timestamp - 5.5 for v in r.values]
        assert r.values[-1].timestamp >= now - 3 - 10


async def test_ranges_fallback(db: victoria.VictoriaClient, now: int):
    # The dense database has none of the fields: the long-term database answers
    names = ['fallback/x']
    minute = now - now % 60
    averages = {n: [(t * 1000, 7.0) for t in range(minute - 3600, minute + 1, 60)] for n in names}
    await import_samples(db._archive, averages)
    await wait_searchable(db._archive, 'fallback', averages)

    [r] = await db.ranges(TimeSeriesRangesQuery(fields=names, duration='10m'))
    timestamps = [v.timestamp for v in r.values]
    assert all(b - a == 60 for a, b in zip(timestamps, timestamps[1:], strict=False))
    assert {v.value for v in r.values} == {'7'}


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
    await import_samples(db._dense, raw)
    await wait_searchable(db._archive, 'csv', averages)
    await wait_searchable(db._dense, 'csv', raw)

    lines = [line async for line in db.csv(TimeSeriesCsvQuery(fields=names, duration='3h', precision='ms'))]
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
    assert all(b - a == 60_000 for a, b in zip(timestamps[:switch], timestamps[1:switch], strict=False))
    assert all(b - a == 10_000 for a, b in zip(timestamps[switch:], timestamps[switch + 1 :], strict=False))
    assert timestamps[switch - 1] == (horizon + (-horizon % 60)) * 1000
    assert timestamps[switch] == timestamps[switch - 1] + 5_000
