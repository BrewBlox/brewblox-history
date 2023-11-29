import logging
import traceback
from datetime import datetime, timedelta, timezone
from functools import lru_cache

import ciso8601
from pytimeparse.timeparse import timeparse

from .models import ServiceConfig

FLAT_SEPARATOR = '/'
DESIRED_POINTS = 1000
DEFAULT_DURATION = timedelta(days=1)

LOGGER = logging.getLogger(__name__)

DatetimeSrc_ = str | int | float | datetime | None


class DuplicateFilter(logging.Filter):
    """
    Logging filter to prevent long-running errors from flooding the log.
    When set, repeated log messages are blocked.
    This will not block alternating messages, and is module-specific.
    """

    def filter(self, record):
        current_log = (record.module, record.levelno, record.msg)
        if current_log != getattr(self, 'last_log', None):
            self.last_log = current_log
            return True
        return False


@lru_cache
def get_config() -> ServiceConfig:
    return ServiceConfig()


def strex(ex: Exception, tb=False):
    """
    Generic formatter for exceptions.
    A formatted traceback is included if `tb=True`.
    """
    msg = f'{type(ex).__name__}({str(ex)})'
    if tb:
        trace = ''.join(traceback.format_exception(None, ex, ex.__traceback__))
        return f'{msg}\n\n{trace}'
    else:
        return msg


def parse_duration(value: str) -> timedelta:
    try:
        return timedelta(seconds=float(value))
    except ValueError:
        return timedelta(seconds=timeparse(value))


def parse_datetime(value: DatetimeSrc_) -> datetime | None:
    if value is None or value == '':
        return None

    elif isinstance(value, datetime):
        return value

    elif isinstance(value, str):
        return ciso8601.parse_datetime(value)

    elif isinstance(value, (int, float)):
        # This is an educated guess
        # 10e10 falls in 1973 if the timestamp is in milliseconds,
        # and in 5138 if the timestamp is in seconds
        if value > 10e10:
            value /= 1000
        return datetime.fromtimestamp(value, tz=timezone.utc)

    else:
        raise ValueError(str(value))


def format_datetime(value: DatetimeSrc_, precision: str = 's') -> str:
    """Formats given date/time value with desired precision.

    Valid `precision` arguments are:
    - ns
    - ms
    - s
    - ISO8601
    """
    dt: datetime | None = parse_datetime(value)

    if dt is None:
        return ''
    elif precision == 'ns':
        return str(int(dt.timestamp() * 1e9))
    elif precision == 'ms':
        return str(int(dt.timestamp() * 1e3))
    elif precision == 's':
        return str(int(dt.timestamp()))
    elif precision == 'ISO8601':
        return dt.isoformat(timespec='milliseconds').replace('+00:00', 'Z')
    else:
        raise ValueError(f'Invalid precision: {precision}')


def is_open_ended(start=None, duration=None, end=None) -> bool:
    """Checks whether given parameters should yield a live response.

    Parameters are considered open-ended if no end date is set:
    either explicitly, or by a combination of start + duration.
    """
    return [bool(start), bool(duration), bool(end)] in [
        [False, False, False],
        [True, False, False],
        [False, True, False],
    ]


def now() -> datetime:  # pragma: no cover
    # You can't mock C extension functions
    # Add a wrapper here so we can mock it
    return datetime.now(timezone.utc)


def select_timeframe(start, duration, end, min_step) -> tuple[str, str, str]:
    """Calculate start, end, and step for given start, duration, and end

    The returned `start` and `end` strings are either empty,
    or contain Unix seconds.

    `duration` is formatted as `{value}s`.
    """
    dt_start: datetime | None = None
    dt_end: datetime | None = None

    if all([start, duration, end]):
        raise ValueError('At most two out of three timeframe arguments can be provided')

    elif not any([start, duration, end]):
        dt_start = now() - DEFAULT_DURATION
        dt_end = None

    elif start and duration:
        dt_start = parse_datetime(start)
        dt_end = dt_start + parse_duration(duration)

    elif start and end:
        dt_start = parse_datetime(start)
        dt_end = parse_datetime(end)

    elif duration and end:
        dt_end = parse_datetime(end)
        dt_start = dt_end - parse_duration(duration)

    elif start:
        dt_start = parse_datetime(start)
        dt_end = None

    elif duration:
        dt_start = now() - parse_duration(duration)
        dt_end = None

    elif end:
        dt_end = parse_datetime(end)
        dt_start = dt_end - DEFAULT_DURATION

    # This path should never be reached
    else:  # pragma: no cover
        raise RuntimeError('Unexpected code path while determining time frame!')

    # Calculate optimal step interval
    # We want a decent resolution without flooding the front-end with data
    actual_duration: timedelta = (dt_end or now()) - dt_start
    desired_step = actual_duration.total_seconds() // DESIRED_POINTS
    step = int(max(desired_step, min_step.total_seconds()))

    return (
        format_datetime(dt_start, 's'),
        format_datetime(dt_end, 's'),
        f'{step}s'
    )
