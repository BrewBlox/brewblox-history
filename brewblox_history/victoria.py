import asyncio
import logging
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from datetime import datetime, timedelta

import httpx
import ujson
from sortedcontainers import SortedDict

from brewblox_history import planner, utils
from brewblox_history.models import (
    TIMESTAMP_TOLERANCE,
    HistoryEvent,
    TimeSeriesCsvQuery,
    TimeSeriesFieldsQuery,
    TimeSeriesMetric,
    TimeSeriesMetricsQuery,
    TimeSeriesRange,
    TimeSeriesRangesQuery,
)

LOGGER = logging.getLogger(__name__)
LOGGER.addFilter(utils.DuplicateFilter())

CV: ContextVar['VictoriaClient'] = ContextVar('victoria.client')

TIMESTAMP_TOLERANCE_MS = TIMESTAMP_TOLERANCE // timedelta(milliseconds=1)

# Series the downsampler imports into the long-term database with every chunk: its cursor.
# Not a field: fields() leaves it out.
MARKER = 'brewblox-history/downsampled'


# Influx line protocol escapes. The database also unescapes `\\`,
# so a backslash is escaped as well, first: a trailing one would escape the separator.
# HistoryEvent refuses what cannot be escaped.
# A chain of replace() calls is several times faster than str.translate() here.
def escape_measurement(name: str) -> str:
    return name.replace('\\', '\\\\').replace(',', '\\,').replace(' ', '\\ ')


def escape_field_key(name: str) -> str:
    return name.replace('\\', '\\\\').replace(',', '\\,').replace('=', '\\=').replace(' ', '\\ ')


@contextmanager
def named_errors(db: httpx.AsyncClient):
    """Transport errors (unreachable, timeout) become a ConnectionError naming the database."""
    try:
        yield
    except httpx.TransportError as ex:
        raise ConnectionError(f'{db.base_url}: {utils.strex(ex)}') from ex


def make_client(protocol: str, host: str, port: int, path: str) -> httpx.AsyncClient:
    config = utils.get_config()
    return httpx.AsyncClient(
        base_url=f'{protocol}://{host}:{port}{path}',
        timeout=httpx.Timeout(5, read=config.victoria_timeout.total_seconds()),
    )


class VictoriaClient:
    def __init__(self):
        config = utils.get_config()

        # Field name -> (value, timestamp)
        self._cached_metrics: dict[str, tuple[float, datetime]] = {}
        # Event keys warned about for timestamps out of tolerance (once per key)
        self._skewed_keys: set[str] = set()

        # The long-term database
        self._archive = make_client(
            config.victoria_protocol,
            config.victoria_host,
            config.victoria_port,
            config.victoria_path,
        )
        self._dense: httpx.AsyncClient | None = None
        self._databases = [self._archive]
        # Receives the raw samples
        self._raw = self._archive

        if config.dense_enabled:
            self._dense = make_client(
                config.dense_protocol,
                config.dense_host,
                config.dense_port,
                config.dense_path,
            )
            self._databases.append(self._dense)
            self._raw = self._dense

        # Unix seconds up to which the long-term database holds averages,
        # once the downsampler knows. Until then, reads assume it keeps up.
        self.cursor: int | None = None
        # Unix seconds from which the dense database has samples, if later than its retention:
        # after it was first enabled or wiped. The long-term database answers before that.
        self.dense_since: int | None = None
        # Reads from the dense database are failing: warned once per outage
        self._dense_reads_failing = False

    def _database(self, db: planner.Database) -> httpx.AsyncClient:
        return self._dense if db == 'dense' else self._archive

    async def close(self):
        # Close every client, also if one fails
        results = await asyncio.gather(*[db.aclose() for db in self._databases], return_exceptions=True)
        for db, result in zip(self._databases, results, strict=True):
            if isinstance(result, BaseException):
                LOGGER.warning(f'{db.base_url}: close failed: {utils.strex(result)}')

    async def _ping(self, db: httpx.AsyncClient):
        with named_errors(db):
            resp = await db.get('/health')
        if resp.text != 'OK':
            raise ConnectionError(f'{db.base_url}: ping returned warning: "{resp.text}"')

    async def ping(self):
        # Report every database that fails
        results = await asyncio.gather(*[self._ping(db) for db in self._databases], return_exceptions=True)
        errors = [utils.strex(result) for result in results if isinstance(result, BaseException)]
        if errors:
            raise ConnectionError(', '.join(errors))

    async def _json_query(self, db: httpx.AsyncClient, url: str, params: dict):
        with named_errors(db):
            resp = await db.post(url, data=params)
        resp.raise_for_status()
        return resp.json()

    async def has_series(self, db: planner.Database, start: int, end: int, selector: str = '{__name__!=""}') -> bool:
        """Whether any selected series has samples between start and end.
        The database answers from its index, without reading samples: it rounds to whole days."""
        resp = await self._json_query(
            self._database(db),
            '/api/v1/series',
            {'match[]': selector, 'start': start, 'end': end, 'limit': 1},
        )
        return bool(resp['data'])

    async def first_timestamp(self, db: planner.Database, window: int, now: int) -> float | None:
        """Unix seconds of the oldest sample in the window before now, over all series."""
        resp = await self._json_query(
            self._database(db),
            '/api/v1/query',
            {'query': f'min(tfirst_over_time({{__name__!=""}}[{window}s]))', 'time': now, 'nocache': 1},
        )
        result = resp['data']['result']
        return float(result[0]['value'][1]) if result else None

    async def last_timestamp(self, db: planner.Database, selector: str, window: int, now: int) -> float | None:
        """Unix seconds of the newest sample in the window before now, over the selected series."""
        resp = await self._json_query(
            self._database(db),
            '/api/v1/query',
            {'query': f'max(tlast_over_time({selector}[{window}s]))', 'time': now, 'nocache': 1},
        )
        result = resp['data']['result']
        return float(result[0]['value'][1]) if result else None

    async def averages(self, start: int, end: int, step: int) -> bytes:
        """The query_range response (JSON) with every dense series' averages over each step, from start to end.

        Every point from start + k * step up to end averages the step before it.
        The points are all at least downsample_lag old: a latency offset that long
        keeps the database from replacing any with an older one."""
        config = utils.get_config()
        with named_errors(self._dense):
            resp = await self._dense.post(
                '/api/v1/query_range',
                data={
                    'query': f'avg_over_time({{__name__!=""}}[{step}s]) keep_metric_names',
                    'start': start,
                    'end': end,
                    'step': f'{step}s',
                    'nocache': 1,
                    'latency_offset': config.downsample_lag.total_seconds(),
                },
            )
        resp.raise_for_status()
        return resp.content

    async def import_archive(self, lines: str):
        """Writes /api/v1/import JSON lines to the long-term database."""
        with named_errors(self._archive):
            resp = await self._archive.post('/api/v1/import', content=lines)
        resp.raise_for_status()

    async def fields(self, args: TimeSeriesFieldsQuery) -> list[str]:
        query = {'match[]': '{__name__!=""}', 'start': f'{args.duration.total_seconds()}s'}
        LOGGER.debug(query)
        results = await asyncio.gather(
            *[self._json_query(db, '/api/v1/series', query) for db in self._databases],
            return_exceptions=True,
        )
        names = set()
        for db, result in zip(self._databases, results, strict=True):
            if isinstance(result, BaseException):
                if db is self._archive:
                    raise result
                # The long-term database lists the same series, at most a few minutes behind
                LOGGER.warning(f'Fields from the long-term database only: {utils.strex(result)}')
            else:
                names.update(v['__name__'] for v in result['data'])
        names.discard(MARKER)
        return sorted(names)

    async def metrics(self, args: TimeSeriesMetricsQuery) -> list[TimeSeriesMetric]:
        start = utils.now() - args.duration
        retv = []
        for field in dict.fromkeys(args.fields):
            cached = self._cached_metrics.get(field)
            if cached and cached[1] >= start:
                retv.append(TimeSeriesMetric(metric=field, value=cached[0], timestamp=cached[1]))
        return retv

    async def _query_ranges(
        self,
        queries: list[planner.RangeQuery],
        names: list[str],
    ) -> tuple[dict[str, list], bool]:
        """Values per series, merged over the queries,
        and whether queries to the dense database failed: those are left out."""
        config = utils.get_config()
        requests = [
            (
                idx,
                self._json_query(
                    self._database(query.db),
                    '/api/v1/query_range',
                    {
                        'query': f'avg_over_time({selector}[{query.step}s]) keep_metric_names',
                        'start': query.start,
                        'end': query.end,
                        'step': f'{query.step}s',
                        # Points in the last query_latency would be replaced by a copy of an older one
                        'latency_offset': config.query_latency.total_seconds(),
                    },
                ),
            )
            for idx, query in enumerate(queries)
            for selector in planner.series_selectors(names)
        ]
        LOGGER.debug(queries)
        responses = await asyncio.gather(*[request for _, request in requests], return_exceptions=True)

        dense_error: BaseException | None = None
        dense_answered = False
        parts = [{} for _ in queries]
        for (idx, _), resp in zip(requests, responses, strict=True):
            if isinstance(resp, BaseException):
                if queries[idx].db != 'dense':
                    raise resp
                dense_error = resp
                continue
            dense_answered = dense_answered or queries[idx].db == 'dense'
            for result in resp['data']['result']:
                parts[idx][result['metric']['__name__']] = result['values']

        # One warning per outage: every graph would repeat it at every refresh
        if dense_error and not self._dense_reads_failing:
            self._dense_reads_failing = True
            LOGGER.warning(f'Ranges without the dense database until it answers: {utils.strex(dense_error)}')
        elif dense_answered and not dense_error and self._dense_reads_failing:
            self._dense_reads_failing = False
            LOGGER.info('Ranges from the dense database again')

        return planner.merge_values(parts), dense_error is not None

    async def ranges(self, args: TimeSeriesRangesQuery) -> list[TimeSeriesRange]:
        config = utils.get_config()
        now = utils.now()
        frame = planner.select_timeframe(args.start, args.duration, args.end, now, config)
        names = list(dict.fromkeys(args.fields))

        queries = planner.plan_ranges(frame, int(now.timestamp()), config, self.cursor, self.dense_since)
        values, dense_failed = await self._query_ranges(queries, names)

        # The long-term database answers when the dense database has none of the fields, or fails.
        # With a long-term part in the plan, that part is the answer.
        if queries and all(q.db == 'dense' for q in queries) and (dense_failed or not values):
            values, _ = await self._query_ranges(planner.plan_fallback(frame, config), names)

        return [TimeSeriesRange(metric={'__name__': name}, values=values[name]) for name in names if name in values]

    async def _export_rows(
        self,
        query: planner.ExportQuery,
        start: int,
        end: int,
        selectors: list[str],
        columns: dict[str, int],
        width: int,
    ) -> SortedDict:
        """Samples between start and end, as rows of `width` by timestamp (ms)."""
        db = self._database(query.db)
        rows = SortedDict()
        params = {
            'match[]': selectors,
            'start': start,
            'end': end,
            'max_rows_per_line': 1000,
        }

        with named_errors(db):
            async with db.stream('POST', '/api/v1/export', data=params) as resp:
                if resp.is_error:
                    await resp.aread()
                    resp.raise_for_status()

                # Objects are returned as newline-separated JSON objects.
                # Metrics may be returned in multiple chunks.
                # We need to transpose incoming (column-based) data to rows.
                async for line in resp.aiter_lines():
                    chunk = ujson.loads(line)
                    field_idx = columns[chunk['metric']['__name__']]
                    empty_row = [''] * width

                    for timestamp, value in zip(chunk['timestamps'], chunk['values']):
                        # We want to avoid creating a new list for every call to setdefault()
                        # We'll re-use the same object until it is inserted
                        row = rows.setdefault(timestamp, empty_row)
                        if row is empty_row:
                            empty_row = [''] * width
                        row[field_idx] = str(value)
        return rows

    async def csv(self, args: TimeSeriesCsvQuery):
        config = utils.get_config()
        now = utils.now()
        frame = planner.select_timeframe(args.start, args.duration, args.end, now, config)
        # Column per field; a repeated field fills its first column
        columns: dict[str, int] = {}
        for idx, field in enumerate(args.fields):
            columns.setdefault(field, idx)
        width = len(args.fields)
        selectors = planner.series_selectors(list(columns))

        # CSV headers
        yield ','.join(['time', *args.fields])

        # CSV values, a window at a time to bound memory.
        # Adjacent windows share their boundary: rows at or before the last one are dropped.
        last = -1
        for query in planner.plan_export(frame, int(now.timestamp()), config, self.dense_since):
            for start, end in planner.chunk_windows(query.start, query.end, query.chunk):
                rows = await self._export_rows(query, start, end, selectors, columns, width)
                for timestamp, row in rows.items():
                    if timestamp > last:
                        last = timestamp
                        yield '{},{}'.format(utils.format_datetime(timestamp, args.precision), ','.join(row))

    def _timestamp(self, evt: HistoryEvent, now: datetime) -> int | None:
        """The event's timestamp in ms, or None if the database should stamp arrival."""
        if evt.timestamp is None:
            return None

        offset = evt.timestamp - utils.to_millis(now)
        if abs(offset) <= TIMESTAMP_TOLERANCE_MS:
            return evt.timestamp

        if evt.key not in self._skewed_keys:
            self._skewed_keys.add(evt.key)
            LOGGER.warning(f'{evt.key}: event timestamp is {offset / 1000:+.1f}s off, using arrival time')
        return None

    async def write(self, evt: HistoryEvent):
        if not evt.data:
            return

        now = utils.now()
        timestamp = self._timestamp(evt, now)
        sampled = now if timestamp is None else utils.from_millis(timestamp)
        line_items = []

        # HistoryEvent sanitized the data: sorted, finite numbers, names that can be escaped
        for field, value in evt.data.items():
            # Database writes are done using the Influx Line Protocol
            # https://docs.influxdata.com/influxdb/v1.7/write_protocols/line_protocol_tutorial/
            line_items.append(f'{escape_field_key(field)}={value}')

            # Local cache used for the metrics API
            self._cached_metrics[f'{evt.key}/{field}'] = (value, sampled)

        try:
            line = f'{escape_measurement(evt.key)} {",".join(line_items)}'
            if timestamp is not None:
                line = f'{line} {timestamp}'
            LOGGER.debug(f'Write: {evt.key}, {len(line_items)} fields')
            resp = await self._raw.post('/write', params={'precision': 'ms'}, content=line)
            # The database rejects the whole line on a parse error
            resp.raise_for_status()

        except httpx.HTTPStatusError as ex:
            LOGGER.warning(f'{self._raw.base_url}: write failed: {utils.strex(ex)}: {ex.response.text}')

        except Exception as ex:
            LOGGER.warning(f'{self._raw.base_url}: write failed: {utils.strex(ex)}')


def setup():
    CV.set(VictoriaClient())


@asynccontextmanager
async def lifespan():
    client = CV.get()
    try:
        yield
    finally:
        await client.close()
