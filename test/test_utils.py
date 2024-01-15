"""
Tests brewblox_history.utils
"""

import logging
from datetime import datetime, timedelta, timezone

import pytest

from brewblox_history import utils

TESTED = utils.__name__


def test_duplicate_filter(caplog: pytest.LogCaptureFixture):
    logger = logging.getLogger('test_duplicate')
    logger.addFilter(utils.DuplicateFilter())
    logger.setLevel(logging.INFO)

    caplog.clear()
    logger.info('message 1')
    logger.info('message 1')
    assert len(caplog.records) == 1
    assert caplog.records[-1].message == 'message 1'

    caplog.clear()
    logger.info('message 2')
    logger.info('message 3')
    logger.info('message 2')
    assert len(caplog.records) == 3
    assert caplog.records[-1].message == 'message 2'


def test_strex():
    try:
        raise RuntimeError('oops')
    except RuntimeError as ex:
        assert utils.strex(ex) == 'RuntimeError(oops)'

        with_tb = utils.strex(ex, tb=True)
        assert with_tb.startswith('RuntimeError(oops)')
        assert 'test_utils.py' in with_tb


def test_parse_duration():
    assert utils.parse_duration('2h10m') == timedelta(hours=2, minutes=10)
    assert utils.parse_duration('10') == timedelta(seconds=10)
    assert utils.parse_duration(timedelta(hours=1)) == timedelta(minutes=60)

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
    iso_str = '2021-07-15T14:29:30Z'
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

    with pytest.raises(ValueError):
        utils.select_timeframe(start='yesterday',
                               duration='2d',
                               end='tomorrow')

    assert utils.select_timeframe(None,
                                  None,
                                  None
                                  ) == (
        fmt(datetime(2021, 7, 14, 19)),
        '',
        '86s'
    )

    assert utils.select_timeframe(start=now(),
                                  duration='1h',
                                  end=None
                                  ) == (
        fmt(now()),
        fmt(datetime(2021, 7, 15, 20)),
        '10s',
    )

    assert utils.select_timeframe(start=now(),
                                  duration=None,
                                  end=datetime(2021, 7, 15, 20)
                                  ) == (
        fmt(now()),
        fmt(datetime(2021, 7, 15, 20)),
        '10s',
    )

    assert utils.select_timeframe(start=None,
                                  duration='1h',
                                  end=datetime(2021, 7, 15, 20)
                                  ) == (
        fmt(now()),
        fmt(datetime(2021, 7, 15, 20)),
        '10s',
    )

    assert utils.select_timeframe(start=datetime(2021, 7, 15, 18),
                                  duration=None,
                                  end=None
                                  ) == (
        fmt(datetime(2021, 7, 15, 18)),
        '',
        '10s',
    )

    assert utils.select_timeframe(start=None,
                                  duration='1h',
                                  end=None
                                  ) == (
        fmt(datetime(2021, 7, 15, 18)),
        '',
        '10s',
    )

    assert utils.select_timeframe(start=None,
                                  duration=None,
                                  end=now()
                                  ) == (
        fmt(datetime(2021, 7, 14, 19)),
        fmt(now()),
        '86s',
    )
