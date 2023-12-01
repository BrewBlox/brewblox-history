"""
Tests brewblox_history.utils
"""

from datetime import datetime, timedelta, timezone

import pytest

from brewblox_history import utils

TESTED = utils.__name__


def test_parse_duration():
    assert utils.parse_duration('2h10m') == timedelta(hours=2, minutes=10)
    assert utils.parse_duration('10') == timedelta(seconds=10)

    with pytest.raises(TypeError):
        utils.parse_duration('')

    with pytest.raises(TypeError):
        utils.parse_duration(None)


def test_parse_datetime():
    time_s = 1626359370
    iso_str = '2021-07-15T14:29:30.000Z'
    time_ms = time_s * 1000
    dt = datetime.fromtimestamp(time_s, tz=timezone.utc)

    assert utils.parse_datetime(time_s) == dt
    assert utils.parse_datetime(time_ms) == dt
    assert utils.parse_datetime(iso_str) == dt
    assert utils.parse_datetime('') is None
    assert utils.parse_datetime(None) is None

    with pytest.raises(ValueError):
        utils.parse_datetime({})


def test_format_datetime():
    time_s = 1626359370
    iso_str = '2021-07-15T14:29:30.000Z'
    time_ms = time_s * 1000
    dt = datetime.fromtimestamp(time_s, tz=timezone.utc)

    assert utils.format_datetime(time_s, 's') == str(time_s)
    assert utils.format_datetime(iso_str, 'ns') == str(int(time_s * 1e9))
    assert utils.format_datetime(time_ms, 'ms') == str(time_ms)
    assert utils.format_datetime(dt) == str(time_s)
    assert utils.format_datetime(time_s, 'ISO8601') == iso_str
    assert utils.format_datetime(None) == ''
    assert utils.format_datetime('') == ''

    with pytest.raises(ValueError):
        utils.format_datetime(time_s, 'jiffies')


def test_select_timeframe(mocker):
    def now() -> datetime:
        return datetime(2021, 7, 15, 19)

    def fmt(dt: datetime) -> str:
        return str(int(dt.timestamp()))

    mocker.patch(TESTED + '.now').side_effect = now
    min_step = timedelta(seconds=10)

    with pytest.raises(ValueError):
        utils.select_timeframe(start='yesterday',
                               duration='2d',
                               end='tomorrow',
                               min_step=min_step)

    assert utils.select_timeframe(None, None, None, min_step) == (
        fmt(datetime(2021, 7, 14, 19)),
        '',
        '86s'
    )

    assert utils.select_timeframe(start=now(),
                                  duration='1h',
                                  end=None,
                                  min_step=min_step
                                  ) == (
        fmt(now()),
        fmt(datetime(2021, 7, 15, 20)),
        '10s',
    )

    assert utils.select_timeframe(start=now(),
                                  duration=None,
                                  end=datetime(2021, 7, 15, 20),
                                  min_step=min_step
                                  ) == (
        fmt(now()),
        fmt(datetime(2021, 7, 15, 20)),
        '10s',
    )

    assert utils.select_timeframe(start=None,
                                  duration='1h',
                                  end=datetime(2021, 7, 15, 20),
                                  min_step=min_step
                                  ) == (
        fmt(now()),
        fmt(datetime(2021, 7, 15, 20)),
        '10s',
    )

    assert utils.select_timeframe(start=datetime(2021, 7, 15, 18),
                                  duration=None,
                                  end=None,
                                  min_step=min_step
                                  ) == (
        fmt(datetime(2021, 7, 15, 18)),
        '',
        '10s',
    )

    assert utils.select_timeframe(start=None,
                                  duration='1h',
                                  end=None,
                                  min_step=min_step
                                  ) == (
        fmt(datetime(2021, 7, 15, 18)),
        '',
        '10s',
    )

    assert utils.select_timeframe(start=None,
                                  duration=None,
                                  end=now(),
                                  min_step=min_step
                                  ) == (
        fmt(datetime(2021, 7, 14, 19)),
        fmt(now()),
        '86s',
    )
