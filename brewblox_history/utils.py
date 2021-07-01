import logging
import time
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Optional, Union
from weakref import WeakSet

from aiohttp import WSCloseCode, web
from brewblox_service import features
from dateutil import parser as dateparser


class DuplicateFilter(logging.Filter):
    def filter(self, record):
        current_log = (record.module, record.levelno, record.msg)
        if current_log != getattr(self, 'last_log', None):
            self.last_log = current_log
            return True
        return False


class SocketCloser(features.ServiceFeature):

    def __init__(self, app: web.Application) -> None:
        super().__init__(app)
        app['websockets'] = WeakSet()

    async def startup(self, app: web.Application):
        pass

    async def before_shutdown(self, app: web.Application):
        for ws in set(app['websockets']):
            await ws.close(code=WSCloseCode.GOING_AWAY,
                           message='Server shutdown')

    async def shutdown(self, app: web.Application):
        pass


def ms_time():
    return time.time_ns() // 1_000_000


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


def parse_datetime(value: Union[str, int, None]) -> Optional[datetime]:
    if isinstance(value, str):
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


def format_datetime(dt: datetime):
    return dt.timestamp()


def select_timeframe(start=None, duration=None, end=None):
    params = {
        'duration': '10m',
        'end': '',
    }

    if all([start, duration, end]):
        raise ValueError('At most two out of three duration arguments can be provided')

    elif not any([start, duration, end]):
        pass

    elif start and duration:
        dt_end = parse_datetime(start) + parse_duration(duration)
        params.update({
            'duration': duration,
            'end': format_datetime(dt_end),
        })

    elif start and end:
        dt_start = parse_datetime(start)
        dt_end = parse_datetime(end)
        dt_duration: timedelta = dt_end - dt_start
        params.update({
            'duration': f'{dt_duration.seconds}s',
            'end': format_datetime(dt_end),
        })

    elif duration and end:
        params.update({
            'duration': duration,
            'end': format_datetime(parse_datetime(end)),
        })

    elif start:
        dt_start = parse_datetime(start)
        dt_end = datetime.now()
        dt_duration: timedelta = dt_end - dt_start
        params.update({
            'duration': f'{dt_duration.seconds}s',
        })

    elif duration:
        params.update({
            'duration': duration,
        })

    elif end:
        pass

    # This path should never be reached
    else:  # pragma: no cover
        raise RuntimeError('Unexpected code path while determining time frame!')

    return params['duration'], params['end']


def try_float(v) -> bool:
    try:
        float(v)
        return True
    except (ValueError, TypeError):
        return False


def setup(app: web.Application):
    features.add(app, SocketCloser(app))
