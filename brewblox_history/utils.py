import time
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Optional, Tuple, Union
from weakref import WeakSet

from aiohttp import WSCloseCode, web
from brewblox_service import brewblox_logger, features
from dateutil import parser as dateparser

DESIRED_POINTS = 1000
DEFAULT_DURATION = timedelta(days=1)
MINIMUM_STEP = timedelta(seconds=10)

LOGGER = brewblox_logger(__name__, True)


class SocketCloser(features.ServiceFeature):

    def __init__(self, app: web.Application) -> None:
        super().__init__(app)
        app['websockets'] = WeakSet()

    async def before_shutdown(self, app: web.Application):
        for ws in set(app['websockets']):
            await ws.close(code=WSCloseCode.GOING_AWAY,
                           message='Server shutdown')


def ms_time():
    return time.time_ns() // 1_000_000


def s_time():
    return time.time()


def parse_duration(value: str) -> timedelta:  # example: '5d3h2m1s'
    value = value.lower()
    total_seconds = Decimal('0')
    prev_num = []
    for character in value:
        if character.isalpha():
            if prev_num:
                num = Decimal(''.join(prev_num))
                if character == 'd':
                    total_seconds += num * 60 * 60 * 24
                elif character == 'h':
                    total_seconds += num * 60 * 60
                elif character == 'm':
                    total_seconds += num * 60
                elif character == 's':
                    total_seconds += num
                prev_num = []
        elif character.isnumeric() or character == '.':
            prev_num.append(character)
    return timedelta(seconds=float(total_seconds))


def parse_datetime(value: Union[str, int, datetime, None]) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value

    elif isinstance(value, str):
        return dateparser.parse(value)

    elif isinstance(value, (int, float)):
        # This is an educated guess
        # 10e10 falls in 1973 if the timestamp is in milliseconds,
        # and in 5138 if the timestamp is in seconds
        if value > 10e10:
            value //= 1000
        return datetime.fromtimestamp(value)

    else:
        return None


def timestamp_str(value: Union[str, int, datetime, None]) -> str:
    dt = parse_datetime(value)
    if dt:
        return str(dt.timestamp())
    else:
        return ''


def format_csv_timestamp(ms: int, precision: Optional[str]) -> str:
    if precision == 'ns':
        return str(ms * 1e6)
    elif precision == 's':
        return str(ms / 1e3)
    elif precision == 'ISO8601':
        return datetime.fromtimestamp(ms / 1e3).isoformat(timespec='milliseconds') + 'Z'
    else:
        return str(ms)


def optformat(fmt: str, v: Optional[Any]) -> str:
    if v:
        return fmt.format(v)
    else:
        return ''


def is_open_ended(start=None, duration=None, end=None, **_) -> bool:
    return [bool(start), bool(duration), bool(end)] in [
        [False, False, False],
        [True, False, False],
        [False, True, False],
    ]


def select_timeframe(start=None, duration=None, end=None) -> Tuple[str, str, str]:
    """Calculate start, end, and step for given start, duration, and end"""
    dt_start: Optional[datetime] = None
    dt_end: Optional[datetime] = None

    if all([start, duration, end]):
        raise ValueError('At most two out of three duration arguments can be provided')

    elif not any([start, duration, end]):
        dt_start = datetime.now() - DEFAULT_DURATION
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
        dt_start = datetime.now() - parse_duration(duration)
        dt_end = None

    elif end:
        dt_end = parse_datetime(end)
        dt_start = dt_end - DEFAULT_DURATION

    # This path should never be reached
    else:  # pragma: no cover
        raise RuntimeError('Unexpected code path while determining time frame!')

    # Calculate optimal step interval
    # We want a decent resolution without flooding the front-end with data
    actual_duration: timedelta = (dt_end or datetime.now()) - dt_start
    desired_step = actual_duration.total_seconds() // DESIRED_POINTS
    step = max(desired_step, MINIMUM_STEP.total_seconds())

    return timestamp_str(dt_start), timestamp_str(dt_end), f'{step}s'


def setup(app: web.Application):
    features.add(app, SocketCloser(app))
