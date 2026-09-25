"""
Pydantic data models
"""

import logging
import math
import re
from collections.abc import Iterable
from datetime import datetime, timedelta
from operator import itemgetter
from typing import Annotated, Any, Literal, NamedTuple, Self

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator
from pydantic.functional_validators import BeforeValidator
from pydantic_core import PydanticCustomError, SchemaValidator, core_schema
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

    source: object = value
    try:
        source = float(value)
    except TypeError:
        source = None
    except ValueError:
        # Zero ('0s') is a valid result
        parsed = timeparse(value)
        source = value if parsed is None else parsed

    return pydantic_timedelta_validator.validate_python(source)


def parse_datetime(value: DatetimeSrc_) -> datetime | None:
    if value is None or value == '':
        return None

    return pydantic_datetime_validator.validate_python(value)


loose_timedelta = Annotated[timedelta, BeforeValidator(parse_duration)]

# One part of a metricsql duration, as in `1d12h`
_RETENTION_PART = re.compile(r'(-?)(\d+(?:\.\d+)?)(ms|s|m|h|d|w|y)')
_RETENTION_UNITS = {
    'ms': timedelta(milliseconds=1),
    's': timedelta(seconds=1),
    'm': timedelta(minutes=1),
    'h': timedelta(hours=1),
    'd': timedelta(days=1),
    'w': timedelta(weeks=1),
    'y': timedelta(days=365),
}


def parse_retention(value: DurationSrc_) -> timedelta:
    """Parses a retention period the way VictoriaMetrics reads `-retentionPeriod`.

    brewblox-ctl gives the database and this service the same value,
    and both must read it as the same duration.
    A bare number or an `M` suffix counts months of 31 days.
    Otherwise the value is a metricsql duration, whose parts can be combined (`1d12h`),
    except that it must not end in `m`: the database refuses that as ambiguous.
    """
    if isinstance(value, timedelta):
        return value

    text = str(value)
    try:
        return timedelta(days=31 * float(text.removesuffix('M')))
    except (ValueError, OverflowError):  # not a number, or beyond timedelta
        pass

    text = text.lower()
    if text.endswith('m') or not re.fullmatch(f'(?:{_RETENTION_PART.pattern})+', text):
        raise ValueError(f'Invalid retention period: {value!r}')

    retention = timedelta()
    negative = False  # As in metricsql: once a part is negative, the parts after it are too
    try:
        for sign, number, unit in _RETENTION_PART.findall(text):
            negative = negative or sign == '-'
            part = float(number) * _RETENTION_UNITS[unit]
            retention += -part if negative else part
    except OverflowError as ex:
        raise ValueError(f'Invalid retention period: {value!r}') from ex
    return retention


loose_retention = Annotated[timedelta, BeforeValidator(parse_retention)]

# Event timestamps further than this from our own clock are replaced by arrival time
TIMESTAMP_TOLERANCE = timedelta(seconds=10)

# The longest the database takes to make a written sample searchable.
# Measured 2-3 s on an unloaded x86 host, 5.3 s for a new series; to re-measure on a Pi (plan B12).
SEARCHABLE_DELAY = timedelta(seconds=6)

# Longest series name (`<key>/<field>`) in UTF-8. Real names stay under ~300 characters
# (the UI allows 200 for a block name); much longer ones would not fit the database's queries.
MAX_NAME_BYTES = 1024

# History fields refused since startup, so that each is logged once
_refused_fields: set[str] = set()
# A refused name is logged up to this many characters
REFUSED_NAME_SHOWN = 200


def refuse_field(name: str) -> None:
    if name not in _refused_fields:
        _refused_fields.add(name)
        shown = name if len(name) <= REFUSED_NAME_SHOWN else f'{name[:REFUSED_NAME_SHOWN]}...'
        LOGGER.warning(f'Refused history field {shown!r}: its name cannot be stored or queried')


def _flatten_into(items: list[tuple[str, Any]], pairs: Iterable[tuple[Any, Any]], parent_key: str) -> None:
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

    # The long-term database. With dense_enabled, it holds `sparse_interval` averages.
    victoria_protocol: Literal['http', 'https'] = 'http'
    victoria_host: str = 'victoria'
    victoria_port: int = 8428
    victoria_path: str = Field(default='/victoria', pattern=r'^(|/.+)$')
    # Read timeout for database requests. Must exceed the database's own
    # query deadline (-search.maxQueryDuration, 30s by default) so that its
    # error is reported instead of a client timeout.
    victoria_timeout: loose_timedelta = timedelta(seconds=60)

    # The dense database: every raw sample, kept for `dense_retention`.
    # Without it, raw samples go to the long-term database.
    dense_enabled: bool = False
    dense_protocol: Literal['http', 'https'] = 'http'
    dense_host: str = 'victoria-dense'
    dense_port: int = 8428
    dense_path: str = Field(default='/victoria-dense', pattern=r'^(|/.+)$')
    # The dense database's -retentionPeriod, in its format
    dense_retention: loose_retention = timedelta(days=30)
    # Queries starting within this of the retention limit go to the long-term database
    dense_margin: loose_timedelta = timedelta(hours=1)

    history_topic: str = 'brewcast/history'
    datastore_topic: str = 'brewcast/datastore'

    # Live ranges streams send the new points this often
    ranges_interval: loose_timedelta = timedelta(seconds=1)
    metrics_interval: loose_timedelta = timedelta(seconds=1)
    minimum_step: loose_timedelta = timedelta(seconds=1)

    query_duration_default: loose_timedelta = timedelta(days=1)
    query_desired_points: int = 1000

    # Resolution of the long-term database: its query steps are multiples of this
    sparse_interval: loose_timedelta = timedelta(seconds=60)
    # Each sparse_interval is averaged once it ended at least this long ago
    downsample_lag: loose_timedelta = timedelta(seconds=30)
    downsample_interval: loose_timedelta = timedelta(seconds=15)
    # Time per query when catching up (at least sparse_interval). Parsing a chunk blocks the event loop
    # (json.loads holds the GIL): an hour of 200 series takes ~6 ms on x86, ~80 ms on a Pi 3.
    downsample_chunk: loose_timedelta = timedelta(hours=1)
    # A warning is logged while the averages end further back than this
    # (at least twice sparse_interval more than they do when keeping up)
    downsample_max_lag: loose_timedelta = timedelta(minutes=10)
    # Open-ended queries end this long before now, and pass it as the latency offset.
    # A point queried before its samples are searchable misses them; a live stream does not send it again.
    # Samples are searchable 2-3 s after the write, and Spark samples arrive up to ~1.3 s after their timestamp.
    query_latency: loose_timedelta = timedelta(seconds=5)
    # Time per request when exporting CSV
    csv_chunk_dense: loose_timedelta = timedelta(hours=6)
    csv_chunk_sparse: loose_timedelta = timedelta(days=7)
    # Largest step of live follow-ups: long graphs still advance this often
    follow_up_step_max: loose_timedelta = timedelta(seconds=10)

    @model_validator(mode='after')
    def check_intervals(self) -> Self:
        zero = timedelta()

        # Used with or without the dense database
        for name in ['query_latency', 'csv_chunk_dense', 'csv_chunk_sparse', 'follow_up_step_max']:
            if getattr(self, name) <= zero:
                raise ValueError(f'{name} must be positive')

        # Only the dense setup uses the others, and minimum_step predates it.
        # Without it, the service must start whatever they are:
        # brewblox-ctl renders sparse_interval and dense_retention either way.
        if self.dense_enabled:
            self._check_dense_intervals()
        return self

    def _check_dense_intervals(self) -> None:
        zero = timedelta()
        second = timedelta(seconds=1)
        for name in [
            'minimum_step',
            'sparse_interval',
            'downsample_interval',
            'downsample_chunk',
            'downsample_max_lag',
        ]:
            if getattr(self, name) <= zero:
                raise ValueError(f'{name} must be positive')
        if self.minimum_step % second or self.sparse_interval % second:
            raise ValueError('minimum_step and sparse_interval must be whole seconds')
        if self.sparse_interval % self.minimum_step:
            raise ValueError('sparse_interval must be a multiple of minimum_step')
        if self.follow_up_step_max > self.sparse_interval:
            raise ValueError('follow_up_step_max must not exceed sparse_interval')
        if self.dense_retention < timedelta(days=1):
            raise ValueError('dense_retention must be at least 1d, the database minimum')
        if not zero <= self.dense_margin < self.dense_retention:
            raise ValueError('dense_margin must be at least 0 and less than dense_retention')
        if self.downsample_lag < TIMESTAMP_TOLERANCE + SEARCHABLE_DELAY:
            # A sample accepted with the oldest allowed timestamp must be searchable by then
            raise ValueError(f'downsample_lag must be at least {TIMESTAMP_TOLERANCE + SEARCHABLE_DELAY}')


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
        if len(v.encode()) > MAX_NAME_BYTES:
            raise ValueError(f'must not be longer than {MAX_NAME_BYTES} bytes')
        return v

    @field_validator('data', mode='before')
    @classmethod
    def sanitize_data(cls, v: object, info: ValidationInfo) -> dict[str, float]:
        # Also other mappings: they would bypass the sanitizing
        if not isinstance(v, dict):
            raise PydanticCustomError('dict_type', 'Input should be an object')
        # Without a valid key, the whole event is refused: its fields are not checked
        key: str | None = info.data.get('key')
        # Bytes left for the field in a series name
        room = MAX_NAME_BYTES - len(key.encode()) - 1 if key is not None else 0
        data = {}
        for field, raw in flatten(v).items():
            try:
                value = float(raw)
            except (ValueError, TypeError, OverflowError):
                continue
            if not math.isfinite(value) or key is None:
                continue
            if (
                not field
                or '\n' in field
                # A '"' switches the database to quoted-value parsing for the rest of the line
                or '"' in field
                # UTF-8 takes at most 4 bytes per character: encode only names that could be too long
                or (len(field) * 4 > room and len(field.encode()) > room)
            ):
                refuse_field(f'{key}/{field}')
                continue
            data[field] = value
        return data

    @field_validator('timestamp', mode='before')
    @classmethod
    def parse_timestamp(cls, v: object) -> int | None:
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
    duration: loose_timedelta = Field(default=timedelta(days=1), examples=['10m', '1d'])


class TimeSeriesMetricsQuery(BaseModel):
    fields: list[str]
    duration: loose_timedelta = Field(default=timedelta(minutes=10), examples=['10m', '1d'])


class TimeSeriesMetric(BaseModel):
    metric: str
    value: float
    timestamp: datetime = Field(examples=['2020-01-01T20:00:00.000Z'])


class TimeSeriesRangesQuery(BaseModel):
    fields: list[str] = Field(examples=[['spark-one/sensor/value[degC]']])
    start: datetime | None = Field(default=None, examples=['2020-01-01T20:00:00.000Z'])
    end: datetime | None = Field(default=None, examples=['2030-01-01T20:00:00.000Z'])
    duration: loose_timedelta | None = Field(default=None, examples=['1d'])


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


class TimeSeriesPingResponse(PingResponse):
    # Seconds since the end of the last averages in the long-term database.
    # None without the dense database, or before the downsampler found them.
    downsample_age: float | None = None


class ErrorResponse(BaseModel):
    error: str
    details: str
