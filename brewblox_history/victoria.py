import asyncio
import json
from datetime import timedelta
from urllib.parse import quote

from aiohttp import web
from aiohttp.client import ClientSession
from brewblox_service import brewblox_logger, features, http, strex
from llist import sllist

from brewblox_history import utils
from brewblox_history.models import (HistoryEvent, ServiceConfig,
                                     TimeSeriesCsvQuery, TimeSeriesFieldsQuery,
                                     TimeSeriesMetric, TimeSeriesMetricsQuery,
                                     TimeSeriesRange, TimeSeriesRangesQuery)

LOGGER = brewblox_logger(__name__, True)


class VictoriaClient(features.ServiceFeature):

    def __init__(self, app: web.Application):
        super().__init__(app)

        config: ServiceConfig = self.app['config']
        self._url = config.victoria_url
        self._query_headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept-Encoding': 'gzip',
        }

        self._minimum_step = timedelta(seconds=config.minimum_step)
        self._cached_metrics: dict[str, TimeSeriesMetric] = {}

    async def ping(self):
        url = f'{self._url}/health'
        async with http.session(self.app).get(url) as resp:
            status = await resp.text()
            if status != 'OK':  # pragma: no branch
                raise ConnectionError(f'Database ping returned warning: "{status}"')

    async def _json_query(self, query: str, url: str, session: ClientSession):
        async with session.post(url,
                                data=query,
                                headers=self._query_headers) as resp:
            return await resp.json()

    async def fields(self, args: TimeSeriesFieldsQuery) -> list[str]:
        url = f'{self._url}/api/v1/series'
        session = http.session(self.app)

        query = f'match[]={{__name__!=""}}&start={args.duration}'
        LOGGER.debug(query)
        result = await self._json_query(query, url, session)
        retv = [
            v['__name__']
            for v in result['data']
        ]
        retv.sort()

        return retv

    async def metrics(self, args: TimeSeriesMetricsQuery) -> list[TimeSeriesMetric]:
        return list((
            v for k, v in self._cached_metrics.items()
            if k in args.fields
        ))

    async def ranges(self, args: TimeSeriesRangesQuery) -> list[TimeSeriesRange]:
        url = f'{self._url}/api/v1/query_range'
        session = http.session(self.app)

        start, end, step = utils.select_timeframe(args.start,
                                                  args.duration,
                                                  args.end,
                                                  self._minimum_step)
        queries = [
            f'query=avg_over_time({{__name__="{quote(f)}"}}[{step}])&step={step}&start={start}&end={end}'
            for f in args.fields
        ]
        LOGGER.debug(queries)
        query_responses = await asyncio.gather(*[
            self._json_query(q, url, session)
            for q in queries
        ])
        retv = [
            TimeSeriesRange(**(resp['data']['result'][0]))
            for resp in query_responses
            if resp['data']['result']
        ]

        return retv

    async def csv(self, args: TimeSeriesCsvQuery):
        url = f'{self._url}/api/v1/export'
        session = http.session(self.app)
        start, end, _ = utils.select_timeframe(args.start,
                                               args.duration,
                                               args.end,
                                               self._minimum_step)
        matches = '&'.join([
            f'match[]={{__name__="{quote(f)}"}}'
            for f in args.fields
        ])
        query = f'{matches}&start={start}&end={end}'
        query += '&reduce_mem_usage=1'
        query += '&max_rows_per_line=1000'

        width = len(args.fields) + 1  # include timestamps
        rows = sllist()

        async with session.post(url,
                                data=query,
                                headers=self._query_headers) as resp:
            # Objects are returned as newline-separated JSON objects.
            # Metrics may be returned in multiple chunks.
            # We need to transpose incoming (column-based) data to rows.
            # This is done by using a linked list of values.
            # Each node in the linked list is a row.
            # For each timestamp/value received from Victoria,
            # we scan the linked list, and either insert or update a row.
            #
            # Because incoming data is sorted, we can retain a pointer to a row node.
            # All values in the same metric only have to scan after the previously inserted/updated row.
            # The pointer must be reset when a new metric starts.
            chunk_field = None
            chunk_prev = None
            chunk_ptr = None

            while line := await resp.content.readline():
                chunk = json.loads(line)
                field = chunk['metric']['__name__']
                field_idx = args.fields.index(field) + 1  # include timestamp

                # If multiple chunks are returned for the same metric,
                # they are guaranteed to be in order.
                # We can start iterating at the last known position.
                if field == chunk_field:
                    prev = chunk_prev
                    ptr = chunk_ptr
                else:
                    chunk_field = field
                    prev = None
                    ptr = rows.first

                for (timestamp, value) in zip(chunk['timestamps'], chunk['values']):
                    while ptr is not None and ptr.value[0] < timestamp:
                        prev = ptr
                        ptr = ptr.next

                    if ptr is None:  # end of list reached
                        arr = [''] * width
                        arr[0] = timestamp
                        arr[field_idx] = str(value)
                        ptr = rows.appendright(arr)

                    elif ptr.value[0] == timestamp:  # existing entry found
                        arr = ptr.value
                        arr[field_idx] = str(value)

                    elif prev is None:  # new row at the very start
                        arr = [''] * width
                        arr[0] = timestamp
                        arr[field_idx] = str(value)
                        rows.appendleft(arr)

                    else:  # new row between prev and ptr
                        arr = [''] * width
                        arr[0] = timestamp
                        arr[field_idx] = str(value)
                        ptr = rows.insertafter(arr, prev)

                chunk_prev = prev
                chunk_ptr = ptr

            # CSV headers
            yield ','.join(['time'] + args.fields)

            # CSV values
            for row in rows.itervalues():
                row[0] = str(utils.format_datetime(row[0], args.precision))
                yield ','.join(row)

    async def write(self, evt: HistoryEvent):
        url = f'{self._url}/write'
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
                await http.session(self.app).post(url, data=line)

            except Exception as ex:
                msg = strex(ex)
                LOGGER.warning(f'{self} {msg}')


def setup(app):
    features.add(app, VictoriaClient(app))


def fget(app: web.Application) -> VictoriaClient:
    return features.get(app, VictoriaClient)
