"""
Pydantic data models
"""

import collections
from datetime import datetime, timedelta
from typing import Any, Literal, NamedTuple

from pydantic import (BaseModel, ConfigDict, Field, field_validator,
                      model_validator)
from pydantic_settings import BaseSettings, SettingsConfigDict


def flatten(d, parent_key=''):
    """Flattens given dict to have a depth of 1 with all values present.

    Nested keys are converted to /-separated paths.
    """
    items: list[tuple[str, Any]] = []
    for k, v in d.items():
        new_key = f'{parent_key}/{k}' if parent_key else str(k)

        if isinstance(v, list):
            v = {li: lv for li, lv in enumerate(v)}

        if isinstance(v, collections.abc.MutableMapping):
            items.extend(flatten(v, new_key).items())
        else:
            items.append((new_key, v))
    return dict(sorted(items, key=lambda pair: pair[0]))


class ServiceConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_file='.appenv',
        env_prefix='brewblox_history_',
        case_sensitive=False,
        json_schema_extra='ignore',
    )

    name: str = 'history'
    debug: bool = False
    debugger: bool = False

    mqtt_protocol: Literal['mqtt', 'mqtts'] = 'mqtt'
    mqtt_host: str = 'eventbus'
    mqtt_port: int = 1883

    redis_host: str = 'redis'
    redis_port: int = 6379

    victoria_protocol: Literal['http', 'https'] = 'http'
    victoria_host: str = 'victoria'
    victoria_port: int = 8428
    victoria_path: str = Field(default='/victoria', pattern=r'^(|/.+)$')

    history_topic: str = 'brewcast/history'
    datastore_topic: str = 'brewcast/datastore'

    ranges_interval: timedelta = timedelta(seconds=10)
    metrics_interval: timedelta = timedelta(seconds=10)
    minimum_step: timedelta = timedelta(seconds=10)

    query_duration_default: timedelta = timedelta(days=1)
    query_desired_points: int = 1000


class HistoryEvent(BaseModel):
    model_config = ConfigDict(
        extra='ignore',
    )

    key: str
    data: dict[str, Any]  # converted to float later

    @field_validator('data', mode='before')
    @classmethod
    def flatten_data(cls, v):
        assert isinstance(v, dict)
        return flatten(v)


class DatastoreValue(BaseModel):
    model_config = ConfigDict(
        extra='allow',
    )

    namespace: str = Field(pattern=r'^[\w\-\.\:~_ \(\)]*$')
    id: str = Field(pattern=r'^[\w\-\.\:~_ \(\)]*$')


class DatastoreSingleQuery(BaseModel):
    namespace: str
    id: str


class DatastoreMultiQuery(BaseModel):
    namespace: str
    ids: list[str] | None = None
    filter: str | None = None


class DatastoreSingleValueBox(BaseModel):
    value: DatastoreValue


class DatastoreOptSingleValueBox(BaseModel):
    value: DatastoreValue | None


class DatastoreMultiValueBox(BaseModel):
    values: list[DatastoreValue]


class DatastoreDeleteResponse(BaseModel):
    count: int


class TimeSeriesFieldsQuery(BaseModel):
    duration: str = Field('1d', examples=['10m', '1d'])


class TimeSeriesMetricsQuery(BaseModel):
    fields: list[str]
    duration: str = Field('10m', examples=['10m', '1d'])


class TimeSeriesMetric(BaseModel):
    metric: str
    value: float
    timestamp: datetime = Field(examples=['2020-01-01T20:00:00.000Z'])


class TimeSeriesRangesQuery(BaseModel):
    fields: list[str] = Field(examples=[['spark-one/sensor/value[degC]']])
    start: datetime | None = Field(None, examples=['2020-01-01T20:00:00.000Z'])
    end: datetime | None = Field(None, examples=['2030-01-01T20:00:00.000Z'])
    duration: str | None = Field(None, examples=['1d'])


class TimeSeriesRangeValue(NamedTuple):
    timestamp: float
    value: str  # Number serialized as string


class TimeSeriesRangeMetric(BaseModel):
    name: str = Field(alias='__name__')


class TimeSeriesRange(BaseModel):
    metric: TimeSeriesRangeMetric
    values: list[TimeSeriesRangeValue]


class TimeSeriesCsvQuery(TimeSeriesRangesQuery):
    precision: Literal['ns', 'ms', 's', 'ISO8601']


class TimeSeriesStreamCommand(BaseModel):
    id: str
    command: Literal['ranges', 'metrics', 'stop']
    query: TimeSeriesRangesQuery | TimeSeriesMetricsQuery | None = None

    @model_validator(mode='before')
    @classmethod
    def check_query_type(cls, data: dict) -> dict:
        command = data.get('command')
        query = data.get('query', {})
        if command == 'ranges':
            data['query'] = TimeSeriesRangesQuery(**query)
        if command == 'metrics':
            data['query'] = TimeSeriesMetricsQuery(**query)
        if command == 'stop':
            data['query'] = None
        return data


class TimeSeriesMetricStreamData(BaseModel):
    metrics: list[TimeSeriesMetric]


class TimeSeriesRangeStreamData(BaseModel):
    initial: bool
    ranges: list[TimeSeriesRange]


class PingResponse(BaseModel):
    ping: Literal['pong'] = 'pong'


class ErrorResponse(BaseModel):
    error: str
    details: str
