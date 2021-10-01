import asyncio
import json
import time
from collections import deque
from dataclasses import dataclass
from typing import NamedTuple, TypedDict
from urllib.parse import quote

from aiohttp import web
from aiohttp.client import ClientSession
from brewblox_service import brewblox_logger, features, http, repeater, strex

from brewblox_history import utils

LOGGER = brewblox_logger(__name__, True)
MAX_PENDING_LINES = 5000


@dataclass
class CsvColumn:
    metric: str
    idx: int
    timestamps: deque[float]
    values: deque[str]


class Metric(TypedDict):
    metric: str
    value: float
    timestamp: int


class RangeValue(NamedTuple):
    timestamp: int
    value: str


class Range(TypedDict):
    metric: dict[str, str]  # { __name__: string }
    values: list[RangeValue]


class VictoriaClient(repeater.RepeaterFeature):

    def __init__(self, app: web.Application):
        super().__init__(app)

        config = self.app['config']
        self._url = config['victoria_url']
        self._query_headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept-Encoding': 'gzip',
        }

        self._write_interval = config['write_interval']
        self._last_err = 'init'
        self._pending_lines: list[str] = []
        self._cached_metrics: dict[str, Metric] = {}

    async def ping(self):
        url = f'{self._url}/health'
        async with http.session(self.app).get(url) as resp:
            status = await resp.text()
            if status != 'OK':
                raise ConnectionError(f'Database ping returned warning: "{status}"')

    async def _json_query(self, query: str, url: str, session: ClientSession):
        async with session.post(url,
                                data=query,
                                headers=self._query_headers) as resp:
            return await resp.json()

    async def fields(self, duration: str) -> list[str]:
        url = f'{self._url}/api/v1/series'
        session = http.session(self.app)

        query = f'match[]={{__name__!=""}}&start={duration}'
        result = await self._json_query(query, url, session)
        retv = [
            v['__name__']
            for v in result['data']
        ]
        retv.sort()

        return retv

    async def metrics(self, fields: list[str]) -> list[Metric]:
        return list((
            v for k, v in self._cached_metrics.items()
            if k in fields
        ))

    async def ranges(self,
                     fields: list[str],
                     start: str = None,
                     end: str = None,
                     duration: str = None,
                     ) -> list[Range]:
        url = f'{self._url}/api/v1/query_range'
        session = http.session(self.app)

        start, end, step = utils.select_timeframe(start, duration, end)
        queries = [
            f'query=avg_over_time({{__name__="{quote(f)}"}}[{step}])&step={step}&start={start}&end={end}'
            for f in fields
        ]
        LOGGER.debug(queries)
        result = await asyncio.gather(*[
            self._json_query(q, url, session)
            for q in queries
        ])
        retv = [
            v['data']['result'][0]
            for v in result
            if v['data']['result']
        ]

        return retv

    async def csv(self,
                  fields: list[str],
                  start: str = None,
                  end: str = None,
                  duration: str = None,
                  precision: str = None,
                  ):
        url = f'{self._url}/api/v1/export'
        session = http.session(self.app)
        start, end, _ = utils.select_timeframe(start, duration, end)
        matches = '&'.join([
            f'match[]={{__name__="{quote(f)}"}}'
            for f in fields
        ])
        query = f'{matches}&start={start}&end={end}&reduce_mem_usage=1'
        cols: list[CsvColumn] = []

        async with session.post(url,
                                data=query,
                                headers=self._query_headers) as resp:
            # Objects are returned as newline-separated JSON objects
            # Metrics may be returned in multiple chunks
            async for line in resp.content:  # pragma: no branch
                parsed = json.loads(line)
                field = parsed['metric']['__name__']
                cols.append(CsvColumn(
                    metric=field,
                    idx=fields.index(field),
                    timestamps=deque(parsed['timestamps']),
                    values=deque(parsed['values']),
                ))

        # CSV headers
        yield ','.join(['time'] + fields)

        # CSV values
        # Scan for earliest timestamp
        # Then collect all values with that timestamp
        while cols:
            # The database returned sorted lists of timestamps
            # Find the earliest timestamp
            timestamp = min((c.timestamps[0] for c in cols))

            # Pre-populate with formatted timestamp and empty strings
            t = utils.format_datetime(timestamp, precision)
            output = [t] + [''] * len(fields)

            for col in cols:
                # If timestamp matches, insert at the correct position in output
                # Pop both value and timestamp so the next iteration can look
                # for the next earliest timestamp
                if col.timestamps[0] == timestamp:
                    col.timestamps.popleft()
                    output[col.idx + 1] = str(col.values.popleft())

            yield ','.join(output)

            # The loop ends when all cols are exhausted
            cols = [c for c in cols if c.timestamps]

    def _avoid_overflow(self):
        # Ensure that a disconnected database does not cause this service to run out of memory
        # To avoid large gaps, the data is downsampled: only every 2nd element is kept
        # Note: using this approach, resolution decreases with age (downsampled more often)
        if len(self._pending_lines) >= MAX_PENDING_LINES:
            LOGGER.warn('Downsampling pending points...')
            self._pending_lines = self._pending_lines[::2]

    async def run(self):
        session = http.session(self.app)
        url = f'{self._url}/write'

        while True:
            await asyncio.sleep(self._write_interval)

            if not self._pending_lines:
                continue

            points = self._pending_lines.copy()
            LOGGER.debug(f'Pushing {len(points)} lines to Victoria')

            try:
                await session.get(url, data='\n'.join(points))

                # Make sure to keep points that were inserted during the write
                self._pending_lines = self._pending_lines[len(points):]

                if self._last_err:
                    LOGGER.info(f'{self} now active')
                    self._last_err = None

            except Exception as ex:
                msg = strex(ex)
                LOGGER.warning(f'{self} {msg}')
                self._last_err = msg
                self._avoid_overflow()

    def write_soon(self,
                   service: str,
                   points: dict[str, float]):
        time_ns = time.time_ns()
        time_ms = time_ns // 1_000_000
        line_values = []

        for field, raw_value in points.items():
            try:
                metric = f'{service}/{field}'
                value = float(raw_value)

                # Database writes are done using the Influx Line Protocol
                # https://docs.influxdata.com/influxdb/v1.7/write_protocols/line_protocol_tutorial/
                line_values.append(f'{field}={value}')

                # Local cache used for the metrics API
                self._cached_metrics[metric] = Metric(
                    metric=metric,
                    value=value,
                    timestamp=time_ms
                )

            except (ValueError, TypeError):
                pass  # Skip values that can't be converted to float

        # Append a line if `points` contained any valid values
        if line_values:
            formatted_values = ','.join(line_values).replace(' ', '\\ ')
            self._pending_lines.append(f'{service} {formatted_values} {time_ns}')


def setup(app):
    features.add(app, VictoriaClient(app))


def fget(app: web.Application) -> VictoriaClient:
    return features.get(app, VictoriaClient)
