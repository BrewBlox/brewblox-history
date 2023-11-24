"""
Pydantic data models
"""

import collections
from datetime import datetime
from typing import Any, Literal, NamedTuple

from pydantic import BaseModel, ConfigDict, Field, model_validator
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
        env_prefix='brewblox_',
        case_sensitive=False,
        extra='ignore',
    )

    name: str = 'history'
    debug: bool = False
    redis_url: str = 'redis://redis'
    victoria_url: str = 'http://victoria:8428/victoria'
    history_topic: str = 'brewcast/history'
    datastore_topic: str = 'brewcast/datastore'
    ranges_interval: float = 10
    metrics_interval: float = 10
    minimum_step: float = 10


class HistoryEvent(BaseModel):
    model_config = ConfigDict(extra='ignore')

    key: str
    data: dict[str, Any]  # converted to float later

    @model_validator(mode='before')
    @classmethod
    def flatten_data(cls, v):
        assert isinstance(v, dict)
        return flatten(v)


class DatastoreValue(BaseModel):
    model_config = ConfigDict(extra='allow')

    namespace: str
    id: str


class DatastoreCheckedValue(DatastoreValue):
    namespace: str = Field(pattern=r'^[\w\-\.\:~_ \(\)]*$')
    id: str = Field(pattern=r'^[\w\-\.\:~_ \(\)]*$')


class DatastoreSingleQuery(BaseModel):
    namespace: str
    id: str


class DatastoreMultiQuery(BaseModel):
    namespace: str
    ids: list[str] | None
    filter: str | None


class DatastoreSingleValueBox(BaseModel):
    value: DatastoreCheckedValue


class DatastoreOptSingleValueBox(BaseModel):
    value: DatastoreCheckedValue | None


class DatastoreMultiValueBox(BaseModel):
    values: list[DatastoreCheckedValue]


class DatastoreDeleteResponse(BaseModel):
    count: int


class TimeSeriesFieldsQuery(BaseModel):
    duration: str


class TimeSeriesMetricsQuery(BaseModel):
    fields: list[str]


class TimeSeriesMetric(BaseModel):
    metric: str
    value: float
    timestamp: datetime = Field(example='2020-01-01T20:00:00.000Z')


class TimeSeriesRangesQuery(BaseModel):
    fields: list[str] = Field(example=['spark-one/sensor/value[degC]'])
    start: datetime | None = Field(example='2020-01-01T20:00:00.000Z')
    end: datetime | None = Field(example='2030-01-01T20:00:00.000Z')
    duration: str | None = Field(example='1d')


class TimeSeriesRangeValue(NamedTuple):
    timestamp: int
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
    query: dict | None


class TimeSeriesMetricStreamData(BaseModel):
    metrics: list[TimeSeriesMetric]


class TimeSeriesRangeStreamData(BaseModel):
    initial: bool
    ranges: list[TimeSeriesRange]
