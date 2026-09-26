"""
Query planning: which database answers which part of a time range.

These are pure functions of the request, the clock, the downsampler's cursor and the config.
Times are integer Unix seconds.

With dense_enabled, the dense database holds every raw sample for dense_retention
(or since dense_since, if it started later), and the long-term database holds averages
of sparse_interval up to the downsampler's cursor.
A query with a step below sparse_interval that starts where the dense database has samples goes to it.
Otherwise its step is rounded up to a multiple of sparse_interval, on the epoch grid:
averaging the long-term averages at any other step would alias.
The long-term database then answers up to the cursor, and the dense database the rest.

Without dense_enabled, the long-term database holds the raw samples and answers everything.

A live stream sends the initial ranges once, then follow-ups: the points after the last one it sent,
at a step capped at follow_up_step_max, from the database that receives the raw samples.
"""

import math
from datetime import datetime, timedelta
from typing import Literal, NamedTuple, cast

from .models import SEARCHABLE_DELAY, DatetimeSrc_, DurationSrc_, ServiceConfig, parse_datetime, parse_duration

Database = Literal['archive', 'dense']

# A series selector holds at most this many names, and this many bytes:
# the database refuses queries over 16 KiB (-search.maxQueryLen)
SELECTOR_MAX_NAMES = 100
SELECTOR_MAX_BYTES = 8 * 1024

# A live follow-up asks for at most this many points: after a long wait (the database was down,
# the clock jumped forward) or for the first follow-up of a very long graph, it skips the older ones.
# The database refuses more than 30000 points per series (-search.maxPointsPerTimeseries).
FOLLOW_UP_MAX_POINTS = 1000

# A live stream starts over when the clock went back further than this (seconds), and otherwise waits
# for the clock to catch up: an NTP correction goes unnoticed, a wrong time zone set right does not freeze it.
CLOCK_STEP_TOLERANCE = 60


class Timeframe(NamedTuple):
    start: int
    end: int
    step: int


class RangeQuery(NamedTuple):
    db: Database
    start: int
    end: int
    step: int


class ExportQuery(NamedTuple):
    db: Database
    start: int
    end: int
    # Exports are split in windows of this length, to bound memory
    chunk: int


def seconds(value: timedelta) -> int:
    return math.floor(value.total_seconds())


def set_datetime(value: DatetimeSrc_) -> datetime:
    """A start or end that is set: it parses to a datetime, or raises."""
    return cast('datetime', parse_datetime(value))


def chunk_seconds(config: ServiceConfig) -> int:
    """The time per query when averaging into the long-term database: downsample_chunk,
    at least one sparse_interval and in whole ones."""
    interval = seconds(config.sparse_interval)
    chunk = max(seconds(config.downsample_chunk), interval)
    return chunk - chunk % interval


def select_timeframe(
    start: DatetimeSrc_,
    duration: DurationSrc_ | None,
    end: DatetimeSrc_,
    now: datetime,
    config: ServiceConfig,
) -> Timeframe:
    """Start, end and step for given start, duration and end.

    The timeframe ends query_latency before now at the latest:
    the database may not have made newer samples searchable yet,
    and replaces points in the last query_latency with a copy of an older one.
    The step gives about query_desired_points points, and at least minimum_step.
    """
    dt_start: datetime
    dt_end: datetime | None = None

    if all([start, duration, end]):
        raise ValueError('At most two out of three timeframe arguments can be provided')

    if start and duration:
        dt_start = set_datetime(start)
        dt_end = dt_start + parse_duration(duration)

    elif start and end:
        dt_start = set_datetime(start)
        dt_end = set_datetime(end)

    elif duration and end:
        dt_end = set_datetime(end)
        dt_start = dt_end - parse_duration(duration)

    elif start:
        dt_start = set_datetime(start)

    elif duration:
        dt_start = now - parse_duration(duration)

    elif end:
        dt_end = set_datetime(end)
        dt_start = dt_end - config.query_duration_default

    else:
        dt_start = now - config.query_duration_default

    frame_start = math.floor(dt_start.timestamp())
    frame_end = live_end(now, config)
    if dt_end is not None:
        frame_end = min(frame_end, math.floor(dt_end.timestamp()))
    desired_step = (frame_end - frame_start) // config.query_desired_points
    step = max(desired_step, seconds(config.minimum_step), 1)
    return Timeframe(frame_start, frame_end, step)


def steady_cursor(now: int, config: ServiceConfig) -> int:
    """Where the downsampler's cursor is at the latest when it keeps up: the end of the last interval
    whose average is searchable. An interval is averaged at the first tick at least downsample_lag
    after it ended, and its average is searchable a little later."""
    interval = seconds(config.sparse_interval)
    cursor = now - seconds(config.downsample_lag + config.downsample_interval + SEARCHABLE_DELAY)
    return cursor - cursor % interval


def dense_horizon(now: int, config: ServiceConfig, dense_since: int | None) -> int:
    """Where the dense database's samples start, with dense_margin to spare."""
    horizon = now - seconds(config.dense_retention) + seconds(config.dense_margin)
    return horizon if dense_since is None else max(horizon, dense_since)


def plan_ranges(
    frame: Timeframe,
    now: int,
    config: ServiceConfig,
    cursor: int | None = None,
    dense_since: int | None = None,
) -> list[RangeQuery]:
    """The queries that together answer a ranges request for the timeframe.

    cursor is where the long-term database's averages end (steady_cursor if not known),
    and dense_since where the dense database's samples start, if later than its retention.

    The queries are ordered, and their points (start + k * step, up to end) do not overlap.
    Each point averages the step before it, so the first point may precede frame.start.
    """
    start, end, step = frame
    if start > end:
        return []

    if not config.dense_enabled:
        return [RangeQuery('archive', start - start % step, end, step)]

    interval = seconds(config.sparse_interval)
    if cursor is None:
        cursor = steady_cursor(now, config)
    if step < interval and start >= dense_horizon(now, config, dense_since):
        return [RangeQuery('dense', start - start % step, end, step)]

    step = math.ceil(step / interval) * interval
    start -= start % step
    # The last point whose whole step the long-term database has
    seam = min(cursor - cursor % step, end)
    queries = []
    if start <= seam:
        queries.append(RangeQuery('archive', start, seam, step))
    # Both are on the grid of step: after the seam, or at the start if the seam precedes it
    dense_start = max(start, seam + step)
    if dense_start <= end:
        queries.append(RangeQuery('dense', dense_start, end, step))
    return queries


def plan_fallback(frame: Timeframe, config: ServiceConfig) -> list[RangeQuery]:
    """For when the dense database has none of the requested fields, or fails.
    The long-term database answers everything, at a multiple of its interval."""
    interval = seconds(config.sparse_interval)
    step = max(frame.step, interval)
    return plan_ranges(frame._replace(step=step), frame.end, config, cursor=frame.end)


class FollowUp(NamedTuple):
    """Where a live stream continues: with the points after last, at step.
    until is where the last query ended."""

    last: int
    step: int
    until: int


def live_end(now: datetime, config: ServiceConfig) -> int:
    """Where an open-ended query ends: query_latency before now."""
    return math.floor((now - config.query_latency).timestamp())


def follow_up_after(frame: Timeframe, sent: int | None, config: ServiceConfig) -> FollowUp:
    """Where live follow-ups continue after the initial ranges for frame: after the newest point sent,
    or after the frame's start if none was. Not after the planned end: a part may have failed,
    or the fallback may have had nothing towards the end, and the follow-ups fill that in.

    Their step is the frame's step, capped at follow_up_step_max so that long graphs still advance,
    and at least minimum_step, which wins over the cap."""
    step = max(seconds(config.minimum_step), min(frame.step, seconds(config.follow_up_step_max)), 1)
    return FollowUp(frame.start if sent is None else sent, step, frame.end)


def clock_went_back(follow: FollowUp, now: datetime, config: ServiceConfig) -> bool:
    """Whether the clock went back further than CLOCK_STEP_TOLERANCE since the last query ended.
    The points after follow.last would then come after it only when the clock catches up."""
    return live_end(now, config) < follow.until - CLOCK_STEP_TOLERANCE


def plan_follow_up(follow: FollowUp, now: datetime, config: ServiceConfig) -> RangeQuery | None:
    """The live follow-up: the points after follow.last, up to query_latency before now,
    at most FOLLOW_UP_MAX_POINTS of them. None while the next point is not due,
    also while the clock catches up after it went back a little: the points stay in order.

    It reads the database that receives the raw samples, which has them first.
    It ends on its last point, where the next follow-up continues."""
    end = live_end(now, config)
    start = follow.last + follow.step
    if start > end:
        return None
    end -= (end - start) % follow.step
    start = max(start, end - (FOLLOW_UP_MAX_POINTS - 1) * follow.step)
    return RangeQuery('dense' if config.dense_enabled else 'archive', start, end, follow.step)


def plan_export(
    frame: Timeframe,
    now: int,
    config: ServiceConfig,
    dense_since: int | None = None,
) -> list[ExportQuery]:
    """The exports that together answer a CSV request for the timeframe.

    Exports want the raw samples: the dense database answers wherever it has them,
    and the long-term database (averages of sparse_interval) before that.
    They switch on the interval's grid: the average stamped at the switch covers the interval
    before it, and the dense database answers from there.
    """
    start, end, _ = frame
    if start > end:
        return []

    dense_chunk = max(seconds(config.csv_chunk_dense), 1)
    if not config.dense_enabled:
        # The long-term database holds the raw samples
        return [ExportQuery('archive', start, end, dense_chunk)]

    sparse_chunk = max(seconds(config.csv_chunk_sparse), 1)
    horizon = dense_horizon(now, config, dense_since)
    switch = horizon + (-horizon % seconds(config.sparse_interval))
    queries = []
    if start < switch:
        queries.append(ExportQuery('archive', start, min(end, switch), sparse_chunk))
    if end >= switch:
        queries.append(ExportQuery('dense', max(start, switch), end, dense_chunk))
    return queries


def chunk_windows(start: int, end: int, chunk: int) -> list[tuple[int, int]]:
    """Consecutive windows of at most chunk that cover start to end.
    Adjacent windows share their boundary."""
    if start > end:
        return []
    windows = []
    while True:
        stop = min(start + chunk, end)
        windows.append((start, stop))
        if stop >= end:
            return windows
        start = stop


def quote(value: str) -> str:
    """A MetricsQL string literal. The database unquotes it with Go's rules."""
    return '"' + value.replace('\\', '\\\\').replace('"', '\\"') + '"'


def series_selectors(names: list[str]) -> list[str]:
    """Exact-match selectors for the given series names, split to fit the database's limits."""
    selectors = []
    terms: list[str] = []
    size = 0
    for name in names:
        term = f'__name__={quote(name)}'
        term_size = len(term.encode()) + len(' or ')
        if terms and (len(terms) == SELECTOR_MAX_NAMES or size + term_size > SELECTOR_MAX_BYTES):
            selectors.append('{' + ' or '.join(terms) + '}')
            terms, size = [], 0
        terms.append(term)
        size += term_size
    if terms:
        selectors.append('{' + ' or '.join(terms) + '}')
    return selectors


def merge_values(parts: list[dict[str, list]]) -> dict[str, list]:
    """Joins each series' values from consecutive queries.
    Values are [timestamp, value] pairs; a value at or before the previous one is dropped."""
    merged: dict[str, list] = {}
    for part in parts:
        for name, values in part.items():
            existing = merged.setdefault(name, [])
            last = existing[-1][0] if existing else -math.inf
            existing.extend(v for v in values if v[0] > last)
    return merged
