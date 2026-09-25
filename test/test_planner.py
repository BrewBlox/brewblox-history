"""
Tests brewblox_history.planner
"""

import random
from datetime import datetime, timedelta, timezone

import pytest

from brewblox_history import planner
from brewblox_history.models import ServiceConfig
from brewblox_history.planner import ExportQuery, RangeQuery, Timeframe

# A multiple of every step used below (86400 * 11 * 19 * 94)
NOW = 1_697_414_400
HOUR = 3600
DAY = 24 * HOUR


@pytest.fixture
def dense(config: ServiceConfig) -> ServiceConfig:
    config.dense_enabled = True
    config.minimum_step = timedelta(seconds=1)
    return config


def test_select_timeframe(config: ServiceConfig):
    now = datetime(2021, 7, 15, 19, tzinfo=timezone.utc)
    ts = int(now.timestamp())
    latency = 3  # default query_latency

    def select(start=None, duration=None, end=None) -> Timeframe:
        return planner.select_timeframe(start, duration, end, now, config)

    with pytest.raises(ValueError):
        select(start='yesterday', duration='2d', end='tomorrow')

    # Without an end, the timeframe ends query_latency before now
    assert select() == (ts - DAY, ts - latency, 86)
    assert select(start=now - timedelta(hours=1)) == (ts - HOUR, ts - latency, 10)
    assert select(duration='1h') == (ts - HOUR, ts - latency, 10)

    assert select(start=now, duration='1h') == (ts, ts + HOUR, 10)
    assert select(start=now, end=now + timedelta(hours=1)) == (ts, ts + HOUR, 10)
    assert select(duration='1h', end=now) == (ts - HOUR, ts, 10)
    assert select(end=now) == (ts - DAY, ts, 86)

    # Fractions of seconds are dropped
    assert select(start=now + timedelta(seconds=0.7), duration='1h') == (ts, ts + HOUR, 10)

    # The step gives about query_desired_points, at least minimum_step, and at least 1 s
    config.minimum_step = timedelta(seconds=1)
    assert select(duration='10m') == (ts - 600, ts - latency, 1)
    config.minimum_step = timedelta(seconds=0.5)
    assert select(duration='10m').step == 1


def test_steady_cursor(config: ServiceConfig):
    # The end of the last interval whose average is searchable: at the latest
    # downsample_lag (30 s) + downsample_interval (15 s) + SEARCHABLE_DELAY (6 s) after it ended
    assert planner.steady_cursor(NOW, config) == NOW - 60
    assert planner.steady_cursor(NOW + 50, config) == NOW - 60
    assert planner.steady_cursor(NOW + 51, config) == NOW


def test_dense_horizon(dense: ServiceConfig):
    # Retention minus the margin, or where the dense database's samples start if later
    assert planner.dense_horizon(NOW, dense, None) == NOW - 30 * DAY + HOUR
    assert planner.dense_horizon(NOW, dense, NOW - 40 * DAY) == NOW - 30 * DAY + HOUR
    assert planner.dense_horizon(NOW, dense, NOW - HOUR) == NOW - HOUR


@pytest.mark.parametrize(
    'frame, cursor, expected',
    [
        # Short windows in dense retention: raw samples, at their own step
        ((NOW - 600, NOW - 3, 1), NOW - 60, [('dense', NOW - 600, NOW - 3, 1)]),
        ((NOW - 57_000 + 7, NOW - 3, 57), NOW - 60, [('dense', NOW - 57_000, NOW - 3, 57)]),
        # Longer windows: averages up to the cursor, raw after it, at a multiple of 60 s
        (
            (NOW - DAY, NOW - 3, 86),
            NOW - 600,
            [('archive', NOW - DAY, NOW - 600, 120), ('dense', NOW - 480, NOW - 3, 120)],
        ),
        ((NOW - DAY, NOW - 3, 86), NOW - 60, [('archive', NOW - DAY, NOW - 120, 120)]),
        # 604 s rounds up to 660 s; the start rounds down to its grid
        ((NOW - 7 * DAY + 5, NOW - 3, 604), NOW - 60, [('archive', NOW - 7 * DAY - 420, NOW - 660, 660)]),
        # A step of exactly 60 s is on the long-term database's grid
        (
            (NOW - 600, NOW - 3, 60),
            NOW - 300,
            [('archive', NOW - 600, NOW - 300, 60), ('dense', NOW - 240, NOW - 3, 60)],
        ),
        # The cursor before the start: all raw
        ((NOW - 600, NOW - 3, 60), NOW - 1200, [('dense', NOW - 600, NOW - 3, 60)]),
        # The cursor after the end: all averages
        ((NOW - 2 * DAY, NOW - DAY, 86), NOW - 60, [('archive', NOW - 2 * DAY, NOW - DAY, 120)]),
        # Short windows before dense retention: averages
        ((NOW - 40 * DAY, NOW - 40 * DAY + 600, 1), NOW - 60, [('archive', NOW - 40 * DAY, NOW - 40 * DAY + 600, 60)]),
        # Starting within dense_margin of the retention limit: averages
        (
            (NOW - 30 * DAY + 1800, NOW - 30 * DAY + 3600, 2),
            NOW - 60,
            [('archive', NOW - 30 * DAY + 1800, NOW - 30 * DAY + 3600, 60)],
        ),
        (
            (NOW - 30 * DAY + 3600, NOW - 30 * DAY + 5400, 2),
            NOW - 60,
            [('dense', NOW - 30 * DAY + 3600, NOW - 30 * DAY + 5400, 2)],
        ),
        # Nothing to query
        ((NOW - 3, NOW - 600, 1), NOW - 60, []),
    ],
)
def test_plan_ranges(dense: ServiceConfig, frame: tuple, cursor: int, expected: list):
    assert planner.plan_ranges(Timeframe(*frame), NOW, dense, cursor) == [RangeQuery(*q) for q in expected]


def test_plan_ranges_defaults(dense: ServiceConfig):
    # Without a known cursor, the steady cursor
    frame = Timeframe(NOW - DAY, NOW + 120, 86)
    assert planner.plan_ranges(frame, NOW, dense) == planner.plan_ranges(frame, NOW, dense, NOW - 60)


def test_plan_ranges_dense_since(dense: ServiceConfig):
    # Before the dense database's samples start, the long-term database answers
    frame = Timeframe(NOW - 600, NOW - 3, 1)
    assert planner.plan_ranges(frame, NOW, dense, NOW - 120, dense_since=NOW - 300) == [
        RangeQuery('archive', NOW - 600, NOW - 120, 60),
        RangeQuery('dense', NOW - 60, NOW - 3, 60),
    ]
    assert planner.plan_ranges(frame, NOW, dense, NOW - 120, dense_since=NOW - 600) == [
        RangeQuery('dense', NOW - 600, NOW - 3, 1),
    ]


def test_plan_ranges_single(config: ServiceConfig):
    # Without the dense database, everything comes from the long-term database, at the requested step
    assert planner.plan_ranges(Timeframe(NOW - DAY + 5, NOW - 3, 90), NOW, config) == [
        RangeQuery('archive', NOW - DAY, NOW - 3, 90),
    ]
    assert planner.plan_ranges(Timeframe(NOW - 600, NOW - 3, 10), NOW, config) == [
        RangeQuery('archive', NOW - 600, NOW - 3, 10),
    ]
    assert planner.plan_ranges(Timeframe(NOW, NOW - 3, 10), NOW, config) == []


def test_plan_fallback(dense: ServiceConfig):
    # Everything from the long-term database, at a multiple of its interval
    assert planner.plan_fallback(Timeframe(NOW - 600, NOW - 3, 1), dense) == [
        RangeQuery('archive', NOW - 600, NOW - 60, 60),
    ]
    assert planner.plan_fallback(Timeframe(NOW - DAY, NOW - 3, 86), dense) == [
        RangeQuery('archive', NOW - DAY, NOW - 120, 120),
    ]


@pytest.mark.parametrize('seed', range(20))
def test_plan_ranges_properties(config: ServiceConfig, seed: int):
    rand = random.Random(seed)
    for _ in range(500):
        config.dense_enabled = rand.random() < 0.8
        config.minimum_step = timedelta(seconds=rand.choice([1, 2, 5, 10]))
        config.sparse_interval = config.minimum_step * rand.choice([1, 6, 30, 60])
        config.dense_retention = timedelta(days=rand.choice([1, 30]))
        config.dense_margin = timedelta(hours=1)

        now = NOW + rand.randrange(DAY)
        end = now - rand.choice([3, rand.randrange(40 * DAY)])
        start = end - rand.choice([600, HOUR, DAY, 7 * DAY, rand.randrange(1, 60 * DAY)])
        step = max((end - start) // 1000, planner.seconds(config.minimum_step))
        horizon = now - planner.seconds(config.dense_retention) + planner.seconds(config.dense_margin)
        # The downsampler keeps its cursor inside dense retention
        cursor = rand.randrange(horizon, now + 1)
        # The dense database may start later than its retention, and the downsampler averages what it has
        dense_since = rand.choice([None, rand.randrange(horizon, cursor + 1)])
        horizon = planner.dense_horizon(now, config, dense_since)

        queries = planner.plan_ranges(Timeframe(start, end, step), now, config, cursor, dense_since)
        assert queries, (start, end, step)

        # One step for all, at least the requested one, on its own grid
        qstep = queries[0].step
        assert all(q.step == qstep for q in queries)
        assert qstep >= step
        assert all(q.start % qstep == 0 and q.start <= q.end for q in queries)

        # Points cover start to end: the first one's step holds the start, the last one is the last before the end
        points = [p for q in queries for p in range(q.start, q.end + 1, qstep)]
        assert points[0] <= start < points[0] + qstep
        assert points[-1] <= end < points[-1] + qstep
        # Strictly increasing, with no gap where one query ends and the next starts
        assert points == list(range(points[0], points[-1] + 1, qstep))

        if not config.dense_enabled:
            assert [q.db for q in queries] == ['archive']
            continue

        interval = planner.seconds(config.sparse_interval)
        for q in queries:
            if q.db == 'archive':
                # Averages: on their grid, and only where the downsampler has them
                assert qstep % interval == 0
                assert q.start + (q.end - q.start) // qstep * qstep <= cursor
            else:
                # Raw samples: the first point's step reaches back past the margin at most
                assert q.start > horizon - qstep
        # Averages come before raw samples
        assert [q.db for q in queries] in (['archive'], ['dense'], ['archive', 'dense'])


@pytest.mark.parametrize(
    'frame, expected',
    [
        # Raw samples wherever the dense database has them (30 d, minus the 1 h margin)
        ((NOW - DAY, NOW - 3), [('dense', NOW - DAY, NOW - 3, 6 * HOUR)]),
        ((NOW - 40 * DAY, NOW - 35 * DAY), [('archive', NOW - 40 * DAY, NOW - 35 * DAY, 7 * DAY)]),
        (
            (NOW - 40 * DAY, NOW - 3),
            [
                ('archive', NOW - 40 * DAY, NOW - 30 * DAY + HOUR, 7 * DAY),
                ('dense', NOW - 30 * DAY + HOUR, NOW - 3, 6 * HOUR),
            ],
        ),
        ((NOW - 3, NOW - 600), []),
    ],
)
def test_plan_export(dense: ServiceConfig, frame: tuple, expected: list):
    assert planner.plan_export(Timeframe(*frame, 1), NOW, dense) == [ExportQuery(*q) for q in expected]


def test_plan_export_dense_since(dense: ServiceConfig):
    # The switch is on the interval's grid, at or after where the dense database's samples start:
    # the average stamped there covers the interval before it
    assert planner.plan_export(Timeframe(NOW - 5 * HOUR, NOW, 1), NOW, dense, dense_since=NOW - 3 * HOUR + 30) == [
        ExportQuery('archive', NOW - 5 * HOUR, NOW - 3 * HOUR + 60, 7 * DAY),
        ExportQuery('dense', NOW - 3 * HOUR + 60, NOW, 6 * HOUR),
    ]


def test_plan_export_single(config: ServiceConfig):
    # The long-term database holds the raw samples: raw-sized chunks
    assert planner.plan_export(Timeframe(NOW - 40 * DAY, NOW, 1), NOW, config) == [
        ExportQuery('archive', NOW - 40 * DAY, NOW, 6 * HOUR),
    ]
    # Chunks are at least a second
    config.csv_chunk_dense = timedelta(milliseconds=10)
    assert planner.plan_export(Timeframe(NOW - 10, NOW, 1), NOW, config) == [ExportQuery('archive', NOW - 10, NOW, 1)]


@pytest.mark.parametrize(
    'start, end, chunk, expected',
    [
        (0, 10, 4, [(0, 4), (4, 8), (8, 10)]),
        (0, 8, 4, [(0, 4), (4, 8)]),
        (0, 3, 4, [(0, 3)]),
        (5, 5, 4, [(5, 5)]),
        (6, 5, 4, []),
    ],
)
def test_chunk_windows(start: int, end: int, chunk: int, expected: list):
    assert planner.chunk_windows(start, end, chunk) == expected


@pytest.mark.parametrize(
    'value, expected',
    [
        ('sparkey/sensor/value[degC]', '"sparkey/sensor/value[degC]"'),
        ('a "b" (c)', '"a \\"b\\" (c)"'),
        ('back\\slash\\', '"back\\\\slash\\\\"'),
        ("it's\t°", '"it\'s\t°"'),
    ],
)
def test_quote(value: str, expected: str):
    assert planner.quote(value) == expected


def test_series_selectors():
    assert planner.series_selectors([]) == []
    assert planner.series_selectors(['a', 'b"']) == ['{__name__="a" or __name__="b\\""}']

    # At most SELECTOR_MAX_NAMES names per selector
    names = [f'n{i}' for i in range(250)]
    selectors = planner.series_selectors(names)
    assert [s.count('__name__') for s in selectors] == [100, 100, 50]

    # At most SELECTOR_MAX_BYTES per selector, counted in UTF-8
    names = [f'{i:03d}' + '°' * 199 for i in range(100)]
    selectors = planner.series_selectors(names)
    assert len(selectors) > 1
    assert all(len(s.encode()) <= planner.SELECTOR_MAX_BYTES + 2 for s in selectors)
    assert sum(s.count('__name__') for s in selectors) == 100


def test_merge_values():
    assert planner.merge_values([]) == {}
    assert planner.merge_values(
        [
            {'a': [[1, '1'], [2, '2']], 'b': [[1, '1']]},
            {'a': [[2, 'x'], [3, '3']], 'c': [[3, '3']]},
        ]
    ) == {
        'a': [[1, '1'], [2, '2'], [3, '3']],
        'b': [[1, '1']],
        'c': [[3, '3']],
    }
