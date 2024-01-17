import asyncio
import logging
from contextvars import ContextVar
from urllib.parse import quote

import httpx
import ujson
from sortedcontainers import SortedDict

from brewblox_history import utils
from brewblox_history.models import (HistoryEvent, TimeSeriesCsvQuery,
                                     TimeSeriesFieldsQuery, TimeSeriesMetric,
                                     TimeSeriesMetricsQuery, TimeSeriesRange,
                                     TimeSeriesRangesQuery)

LOGGER = logging.getLogger(__name__)
LOGGER.addFilter(utils.DuplicateFilter())

CV: ContextVar['VictoriaClient'] = ContextVar('victoria.client')


class VictoriaClient:

    def __init__(self):
        config = utils.get_config()

        self._url = ''.join([
            config.victoria_protocol,
            '://',
            config.victoria_host,
            ':',
            str(config.victoria_port),
            config.victoria_path,
        ])
        self._query_headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept-Encoding': 'gzip',
        }

        self._cached_metrics: dict[str, TimeSeriesMetric] = {}
        self._client = httpx.AsyncClient(base_url=self._url)

    async def ping(self):
        resp = await self._client.get('/health')
        if resp.text != 'OK':
            raise ConnectionError(
                f'Database ping returned warning: "{resp.text}"')

    async def _json_query(self, query: str, url: str):
        resp = await self._client.post(url, content=query, headers=self._query_headers)
        return resp.json()

    async def fields(self, args: TimeSeriesFieldsQuery) -> list[str]:
        query = f'match[]={{__name__!=""}}&start={args.duration}'
        LOGGER.debug(query)
        result = await self._json_query(query, '/api/v1/series')
        retv = [
            v['__name__']
            for v in result['data']
        ]
        retv.sort()

        return retv

    async def metrics(self, args: TimeSeriesMetricsQuery) -> list[TimeSeriesMetric]:
        start = utils.now() - utils.parse_duration(args.duration)
        return list((
            v for k, v in self._cached_metrics.items()
            if k in args.fields
            and v.timestamp >= start
        ))

    async def ranges(self, args: TimeSeriesRangesQuery) -> list[TimeSeriesRange]:
        start, end, step = utils.select_timeframe(args.start,
                                                  args.duration,
                                                  args.end)
        queries = [
            f'query=avg_over_time({{__name__="{quote(f)}"}}[{step}])&step={step}&start={start}&end={end}'
            for f in args.fields
        ]
        LOGGER.debug(queries)
        query_responses = await asyncio.gather(*[
            self._json_query(q, '/api/v1/query_range')
            for q in queries
        ])
        retv = [
            TimeSeriesRange(**(resp['data']['result'][0]))
            for resp in query_responses
            if resp['data']['result']
        ]

        return retv

    async def csv(self, args: TimeSeriesCsvQuery):
        start, end, _ = utils.select_timeframe(args.start,
                                               args.duration,
                                               args.end)
        matches = '&'.join([
            f'match[]={{__name__="{quote(f)}"}}'
            for f in args.fields
        ])
        query = f'{matches}&start={start}&end={end}'
        query += '&max_rows_per_line=1000'

        width = len(args.fields)
        rows = SortedDict()

        async with self._client.stream('POST',
                                       '/api/v1/export',
                                       content=query,
                                       headers=self._query_headers) as resp:
            # Objects are returned as newline-separated JSON objects.
            # Metrics may be returned in multiple chunks.
            # We need to transpose incoming (column-based) data to rows.
            async for line in resp.aiter_lines():
                chunk = ujson.loads(line)
                field = chunk['metric']['__name__']
                field_idx = args.fields.index(field)
                empty_row = [''] * width

                for (timestamp, value) in zip(chunk['timestamps'], chunk['values']):
                    # We want to avoid creating a new list for every call to setdefault()
                    # We'll re-use the same object until it is inserted
                    row = rows.setdefault(timestamp, empty_row)
                    if row is empty_row:
                        empty_row = [''] * width
                    row[field_idx] = str(value)

            # CSV headers
            yield ','.join(['time', *args.fields])

            # CSV values
            for (timestamp, row) in rows.items():
                yield '{},{}'.format(utils.format_datetime(timestamp, args.precision),
                                     ','.join(row))

    async def write(self, evt: HistoryEvent):
        line_items = []

        for field in sorted(evt.data.keys()):
            try:
                value = float(evt.data[field])
                line_key = field.replace(' ', '\\ ')
                metrics_key = f'{evt.key}/{field}'

                # Database writes are done using the Influx Line Protocol
                # https://docs.influxdata.com/influxdb/v1.7/write_protocols/line_protocol_tutorial/
                line_items.append(f'{line_key}={value}')

                # Local cache used for the metrics API
                self._cached_metrics[metrics_key] = TimeSeriesMetric(
                    metric=metrics_key,
                    value=value,
                    timestamp=utils.now(),
                )

            except (ValueError, TypeError):
                pass  # Skip values that can't be converted to float

        if line_items:
            try:
                line = f'{evt.key} {",".join(line_items)}'
                LOGGER.debug(f'Write: {evt.key}, {len(line_items)} fields')
                await self._client.post('/write', content=line)

            except Exception as ex:
                msg = utils.strex(ex)
                LOGGER.warning(f'{self} {msg}')


def setup():
    CV.set(VictoriaClient())
