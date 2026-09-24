import asyncio
import logging
from contextlib import asynccontextmanager
from contextvars import ContextVar
from datetime import datetime
from urllib.parse import quote

import httpx
import ujson
from sortedcontainers import SortedDict

from brewblox_history import utils
from brewblox_history.models import (
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

# Event timestamps further than this from our own clock are replaced by arrival time
TIMESTAMP_TOLERANCE_MS = 10_000


# Influx line protocol escapes. The database also unescapes `\\`,
# so a backslash is escaped as well, first: a trailing one would escape the separator.
# HistoryEvent refuses what cannot be escaped.
# A chain of replace() calls is several times faster than str.translate() here.
def escape_measurement(name: str) -> str:
    return name.replace('\\', '\\\\').replace(',', '\\,').replace(' ', '\\ ')


def escape_field_key(name: str) -> str:
    return name.replace('\\', '\\\\').replace(',', '\\,').replace('=', '\\=').replace(' ', '\\ ')


class VictoriaClient:
    def __init__(self):
        config = utils.get_config()

        self._url = ''.join(
            [
                config.victoria_protocol,
                '://',
                config.victoria_host,
                ':',
                str(config.victoria_port),
                config.victoria_path,
            ]
        )
        self._query_headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept-Encoding': 'gzip',
        }

        # Field name -> (value, timestamp)
        self._cached_metrics: dict[str, tuple[float, datetime]] = {}
        # Event keys warned about for timestamps out of tolerance (once per key)
        self._skewed_keys: set[str] = set()
        self._client = httpx.AsyncClient(
            base_url=self._url,
            timeout=httpx.Timeout(5, read=config.victoria_timeout.total_seconds()),
        )

    async def close(self):
        await self._client.aclose()

    async def ping(self):
        resp = await self._client.get('/health')
        if resp.text != 'OK':
            raise ConnectionError(f'Database ping returned warning: "{resp.text}"')

    async def _json_query(self, query: str, url: str):
        resp = await self._client.post(url, content=query, headers=self._query_headers)
        resp.raise_for_status()
        return resp.json()

    async def fields(self, args: TimeSeriesFieldsQuery) -> list[str]:
        query = f'match[]={{__name__!=""}}&start={args.duration.total_seconds()}s'
        LOGGER.debug(query)
        result = await self._json_query(query, '/api/v1/series')
        retv = [v['__name__'] for v in result['data']]
        retv.sort()

        return retv

    async def metrics(self, args: TimeSeriesMetricsQuery) -> list[TimeSeriesMetric]:
        start = utils.now() - args.duration
        retv = []
        for field in dict.fromkeys(args.fields):
            cached = self._cached_metrics.get(field)
            if cached and cached[1] >= start:
                retv.append(TimeSeriesMetric(metric=field, value=cached[0], timestamp=cached[1]))
        return retv

    async def ranges(self, args: TimeSeriesRangesQuery) -> list[TimeSeriesRange]:
        start, end, step = utils.select_timeframe(args.start, args.duration, args.end)
        queries = [
            f'query=avg_over_time({{__name__="{quote(f)}"}}[{step}])&step={step}&start={start}&end={end}'
            for f in args.fields
        ]
        LOGGER.debug(queries)
        query_responses = await asyncio.gather(*[self._json_query(q, '/api/v1/query_range') for q in queries])
        retv = [TimeSeriesRange(**(resp['data']['result'][0])) for resp in query_responses if resp['data']['result']]

        return retv

    async def csv(self, args: TimeSeriesCsvQuery):
        start, end, _ = utils.select_timeframe(args.start, args.duration, args.end)
        matches = '&'.join([f'match[]={{__name__="{quote(f)}"}}' for f in args.fields])
        query = f'{matches}&start={start}&end={end}'
        query += '&max_rows_per_line=1000'

        width = len(args.fields)
        rows = SortedDict()

        async with self._client.stream('POST', '/api/v1/export', content=query, headers=self._query_headers) as resp:
            # Objects are returned as newline-separated JSON objects.
            # Metrics may be returned in multiple chunks.
            # We need to transpose incoming (column-based) data to rows.
            async for line in resp.aiter_lines():
                chunk = ujson.loads(line)
                field = chunk['metric']['__name__']
                field_idx = args.fields.index(field)
                empty_row = [''] * width

                for timestamp, value in zip(chunk['timestamps'], chunk['values']):
                    # We want to avoid creating a new list for every call to setdefault()
                    # We'll re-use the same object until it is inserted
                    row = rows.setdefault(timestamp, empty_row)
                    if row is empty_row:
                        empty_row = [''] * width
                    row[field_idx] = str(value)

            # CSV headers
            yield ','.join(['time', *args.fields])

            # CSV values
            for timestamp, row in rows.items():
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
            resp = await self._client.post('/write', params={'precision': 'ms'}, content=line)
            # The database rejects the whole line on a parse error
            resp.raise_for_status()

        except httpx.HTTPStatusError as ex:
            LOGGER.warning(f'{self} {utils.strex(ex)}: {ex.response.text}')

        except Exception as ex:
            LOGGER.warning(f'{self} {utils.strex(ex)}')


def setup():
    CV.set(VictoriaClient())


@asynccontextmanager
async def lifespan():
    client = CV.get()
    try:
        yield
    finally:
        await client.close()
