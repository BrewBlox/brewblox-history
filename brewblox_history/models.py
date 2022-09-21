"""
Pydantic data models
"""

import collections
from datetime import datetime
from typing import Any, Literal, NamedTuple, Optional

from pydantic import BaseModel, Extra, Field, validator


def flatten(d, parent_key=''):
    """Flattens given dict to have a depth of 1 with all values present.

    Nested keys are converted to /-separated paths.
    """
    items = []
    for k, v in d.items():
        new_key = f'{parent_key}/{k}' if parent_key else str(k)

        if isinstance(v, list):
            v = {li: lv for li, lv in enumerate(v)}

        if isinstance(v, collections.abc.MutableMapping):
            items.extend(flatten(v, new_key).items())
        else:
            items.append((new_key, v))
    return dict(items)


class HistoryEvent(BaseModel, extra=Extra.ignore):
    key: str
    data: dict[str, Any]  # converted to float later

    @validator('data', pre=True)
    def flatten_data(cls, v):
        assert isinstance(v, dict)
        return flatten(v)


class DatastoreValue(BaseModel, extra=Extra.allow):
    namespace: str
    id: str


class DatastoreCheckedValue(DatastoreValue):
    namespace: str = Field(regex=r'^[\w\-\.\:~_ \(\)]*$')
    id: str = Field(regex=r'^[\w\-\.\:~_ \(\)]*$')


class DatastoreSingleQuery(BaseModel):
    namespace: str
    id: str


class DatastoreMultiQuery(BaseModel):
    namespace: str
    ids: Optional[list[str]]
    filter: Optional[str]


class DatastoreSingleValueBox(BaseModel):
    value: DatastoreCheckedValue


class DatastoreOptSingleValueBox(BaseModel):
    value: Optional[DatastoreCheckedValue]


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
    start: Optional[datetime] = Field(example='2020-01-01T20:00:00.000Z')
    end: Optional[datetime] = Field(example='2030-01-01T20:00:00.000Z')
    duration: Optional[str] = Field(example='1d')


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
    query: Optional[dict]


class TimeSeriesMetricStreamData(BaseModel):
    metrics: list[TimeSeriesMetric]


class TimeSeriesRangeStreamData(BaseModel):
    initial: bool
    ranges: list[TimeSeriesRange]
