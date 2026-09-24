"""
Pydantic data models
"""

import logging
import math
from collections.abc import Iterable
from datetime import datetime, timedelta
from operator import itemgetter
from typing import Annotated, Any, Literal, NamedTuple

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator
from pydantic.functional_validators import BeforeValidator
from pydantic_core import SchemaValidator, core_schema
from pydantic_settings import BaseSettings, SettingsConfigDict
from pytimeparse.timeparse import timeparse

LOGGER = logging.getLogger(__name__)

DurationSrc_ = str | int | float | timedelta
DatetimeSrc_ = str | int | float | datetime | None

pydantic_timedelta_validator = SchemaValidator(core_schema.timedelta_schema())
pydantic_datetime_validator = SchemaValidator(core_schema.datetime_schema())


def parse_duration(value: DurationSrc_) -> timedelta:
    if isinstance(value, timedelta):
        return value

    try:
        value = float(value)
    except TypeError:
        value = None
    except ValueError:
        value = timeparse(value) or value

    return pydantic_timedelta_validator.validate_python(value)


def parse_datetime(value: DatetimeSrc_) -> datetime | None:
    if value is None or value == '':
        return None

    return pydantic_datetime_validator.validate_python(value)


loose_timedelta = Annotated[timedelta, BeforeValidator(parse_duration)]

# History fields refused since startup, so that each is logged once
_refused_fields: set[str] = set()


def refuse_field(name: str):
    if name not in _refused_fields:
        _refused_fields.add(name)
        LOGGER.warning(f'Refused history field {name!r}: the database cannot store this name')


def _flatten_into(items: list[tuple[str, Any]], pairs: Iterable[tuple[Any, Any]], parent_key: str):
    for k, v in pairs:
        key = f'{parent_key}/{k}' if parent_key else str(k)
        if isinstance(v, dict):
            _flatten_into(items, v.items(), key)
        elif isinstance(v, list):
            _flatten_into(items, enumerate(v), key)
        else:
            items.append((key, v))


def flatten(d: dict) -> dict[str, Any]:
    """Flattens given dict to have a depth of 1 with all values present.

    Nested keys are converted to /-separated paths, and sorted.
    List items are keyed by their index.
    """
    items: list[tuple[str, Any]] = []
    _flatten_into(items, d.items(), '')
    items.sort(key=itemgetter(0))
    return dict(items)


class ServiceConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_file='.appenv',
        env_prefix='brewblox_history_',
        case_sensitive=False,
        extra='ignore',
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
    # Read timeout for database requests. Must exceed the database's own
    # query deadline (-search.maxQueryDuration, 30s by default) so that its
    # error is reported instead of a client timeout.
    victoria_timeout: loose_timedelta = timedelta(seconds=60)

    history_topic: str = 'brewcast/history'
    datastore_topic: str = 'brewcast/datastore'

    ranges_interval: loose_timedelta = timedelta(seconds=10)
    metrics_interval: loose_timedelta = timedelta(seconds=1)
    minimum_step: loose_timedelta = timedelta(seconds=10)

    query_duration_default: loose_timedelta = timedelta(days=1)
    query_desired_points: int = 1000


class HistoryEvent(BaseModel):
    """
    Values to store, sanitized at ingest.

    Series are named `<key>/<field>`, and written with the Influx line protocol,
    which escapes most characters in names. It cannot express the rest:
    an event with such a key is refused, a field with such a name is dropped.
    """

    model_config = ConfigDict(
        extra='ignore',
    )

    key: str
    # Flattened to /-separated field names. Only finite numbers are kept.
    data: dict[str, float]
    # When the values were sampled, in milliseconds since the Unix epoch.
    # Without it, the database stamps the time the write arrives.
    timestamp: int | None = None

    @field_validator('key')
    @classmethod
    def check_key(cls, v: str) -> str:
        # A leading '#' makes the line a comment, and a newline ends it
        if not v or v.startswith('#') or '\n' in v:
            raise ValueError('must not be empty, start with "#", or contain a newline')
        return v

    @field_validator('data', mode='before')
    @classmethod
    def sanitize_data(cls, v, info: ValidationInfo):
        assert isinstance(v, dict)
        data = {}
        for field, value in flatten(v).items():
            try:
                value = float(value)
            except (ValueError, TypeError, OverflowError):
                continue
            if not math.isfinite(value):
                continue
            # A '"' switches the database to quoted-value parsing for the rest of the line
            if not field or '\n' in field or '"' in field:
                # Without a valid key, the whole event is refused
                if 'key' in info.data:
                    refuse_field(f'{info.data["key"]}/{field}')
                continue
            data[field] = value
        return data

    @field_validator('timestamp', mode='before')
    @classmethod
    def parse_timestamp(cls, v):
        # Publishers were free to send any `timestamp` while it was an unknown field.
        # Anything but a number in float range is ignored instead of refusing the event.
        if isinstance(v, bool) or not isinstance(v, int | float):
            return None
        try:
            return round(float(v))
        except (OverflowError, ValueError):  # huge ints, inf, nan
            return None


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
    duration: loose_timedelta = Field(timedelta(days=1), examples=['10m', '1d'])


class TimeSeriesMetricsQuery(BaseModel):
    fields: list[str]
    duration: loose_timedelta = Field(timedelta(minutes=10), examples=['10m', '1d'])


class TimeSeriesMetric(BaseModel):
    metric: str
    value: float
    timestamp: datetime = Field(examples=['2020-01-01T20:00:00.000Z'])


class TimeSeriesRangesQuery(BaseModel):
    fields: list[str] = Field(examples=[['spark-one/sensor/value[degC]']])
    start: datetime | None = Field(None, examples=['2020-01-01T20:00:00.000Z'])
    end: datetime | None = Field(None, examples=['2030-01-01T20:00:00.000Z'])
    duration: loose_timedelta | None = Field(None, examples=['1d'])


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
