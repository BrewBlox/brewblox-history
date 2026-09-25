import logging
import traceback
from datetime import UTC, datetime
from functools import lru_cache

from .models import (  # noqa: F401 (re-exported)
    DatetimeSrc_,
    DurationSrc_,
    ServiceConfig,
    parse_datetime,
    parse_duration,
)

LOGGER = logging.getLogger(__name__)


class DuplicateFilter(logging.Filter):
    """
    Logging filter to prevent long-running errors from flooding the log.
    When set, repeated log messages are blocked.
    This will not block alternating messages, and is module-specific.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        current_log = (record.module, record.levelno, record.msg)
        if current_log != getattr(self, 'last_log', None):
            self.last_log = current_log
            return True
        return False


@lru_cache
def get_config() -> ServiceConfig:  # pragma: no cover
    return ServiceConfig()


def strex(ex: BaseException, *, tb: bool = False) -> str:
    """
    Generic formatter for exceptions.
    A formatted traceback is included if `tb=True`.
    """
    msg = f'{type(ex).__name__}({ex!s})'
    if tb:
        trace = ''.join(traceback.format_exception(None, ex, ex.__traceback__))
        return f'{msg}\n\n{trace}'
    return msg


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
    if precision == 'ns':
        return str(int(dt.timestamp() * 1e9))
    if precision == 'ms':
        return str(int(dt.timestamp() * 1e3))
    if precision == 's':
        return str(int(dt.timestamp()))
    if precision == 'ISO8601':
        return dt.isoformat(timespec='auto').replace('+00:00', 'Z')
    raise ValueError(f'Invalid precision: {precision}')


def is_open_ended(
    start: DatetimeSrc_ = None,
    duration: DurationSrc_ | None = None,
    end: DatetimeSrc_ = None,
) -> bool:
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
    return datetime.now(UTC)


def to_millis(dt: datetime) -> int:
    """Milliseconds since the Unix epoch"""
    return round(dt.timestamp() * 1000)


def from_millis(value: int) -> datetime:
    """UTC datetime for milliseconds since the Unix epoch"""
    return datetime.fromtimestamp(value / 1000, UTC)
