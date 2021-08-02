import collections
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, Union

import ciso8601
from brewblox_service import brewblox_logger
from pytimeparse.timeparse import timeparse

FLAT_SEPARATOR = '/'
DESIRED_POINTS = 1000
DEFAULT_DURATION = timedelta(days=1)
MINIMUM_STEP = timedelta(seconds=10)

LOGGER = brewblox_logger(__name__, True)

DatetimeSrc_ = Union[str, int, float, datetime, None]


def parse_duration(value: str) -> timedelta:
    try:
        return timedelta(seconds=float(value))
    except ValueError:
        return timedelta(seconds=timeparse(value))


def parse_datetime(value: DatetimeSrc_) -> Optional[datetime]:
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
            value //= 1000
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
    dt: Optional[datetime] = parse_datetime(value)

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


def is_open_ended(start=None, duration=None, end=None, **_) -> bool:
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
    return datetime.now()


def select_timeframe(start=None, duration=None, end=None) -> Tuple[str, str, str]:
    """Calculate start, end, and step for given start, duration, and end

    The returned `start` and `end` strings are either empty,
    or contain Unix seconds.

    `duration` is formatted as `{value}s`.
    """
    dt_start: Optional[datetime] = None
    dt_end: Optional[datetime] = None

    if all([start, duration, end]):
        raise ValueError('At most two out of three duration arguments can be provided')

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
    step = max(desired_step, MINIMUM_STEP.total_seconds())

    return (
        format_datetime(dt_start, 's'),
        format_datetime(dt_end, 's'),
        f'{step}s'
    )


def flatten(d, parent_key=''):
    """Flattens given dict to have a depth of 1 with all values present.

    Nested keys are converted to /-separated paths.
    """
    items = []
    for k, v in d.items():
        new_key = f'{parent_key}/{k}' if parent_key else str(k)

        if isinstance(v, list):
            v = {li: lv for li, lv in enumerate(v)}

        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key).items())
        else:
            items.append((new_key, v))
    return dict(items)
