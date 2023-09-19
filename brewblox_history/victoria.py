import asyncio
from datetime import timedelta
from typing import Optional, TextIO
from urllib.parse import quote

import pandas as pd
import ujson
from aiohttp import web
from aiohttp.client import ClientSession
from brewblox_service import brewblox_logger, features, http, strex

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

    async def csv(self, args: TimeSeriesCsvQuery, out: Optional[TextIO]) -> Optional[str]:
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

        df = pd.DataFrame({'time': []})

        # Objects are returned as newline-separated JSON objects.
        # Metrics may be split over multiple chunks.
        # We need to gradually merge the columns into a 2D table
        async with session.post(url,
                                data=query,
                                headers=self._query_headers) as resp:
            while line := await resp.content.readline():
                chunk = ujson.loads(line)
                field = chunk['metric']['__name__']
                df = df.merge(
                    pd.DataFrame({
                        'time': chunk['timestamps'],
                        field: chunk['values'],
                    }),
                    how='outer')

        df.sort_values(by='time', inplace=True)
        df['time'] = df['time'].map(lambda v: utils.format_datetime(v, args.precision))

        return df.to_csv(out, index=False, columns=['time', *args.fields])

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
