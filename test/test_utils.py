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

    with pytest.raises(ValueError):
        utils.select_timeframe(start='yesterday', duration='2d', end='tomorrow')

    assert utils.select_timeframe() == (
        fmt(datetime(2021, 7, 14, 19)),
        '',
        '86.0s'
    )

    assert utils.select_timeframe(start=now(), duration='1h') == (
        fmt(now()),
        fmt(datetime(2021, 7, 15, 20)),
        '10.0s',
    )

    assert utils.select_timeframe(start=now(), end=datetime(2021, 7, 15, 20)) == (
        fmt(now()),
        fmt(datetime(2021, 7, 15, 20)),
        '10.0s',
    )

    assert utils.select_timeframe(duration='1h', end=datetime(2021, 7, 15, 20)) == (
        fmt(now()),
        fmt(datetime(2021, 7, 15, 20)),
        '10.0s',
    )

    assert utils.select_timeframe(start=datetime(2021, 7, 15, 18)) == (
        fmt(datetime(2021, 7, 15, 18)),
        '',
        '10.0s',
    )

    assert utils.select_timeframe(duration='1h') == (
        fmt(datetime(2021, 7, 15, 18)),
        '',
        '10.0s',
    )

    assert utils.select_timeframe(end=now()) == (
        fmt(datetime(2021, 7, 14, 19)),
        fmt(now()),
        '86.0s',
    )


def test_flatten():
    nested_data = {
        'nest': {
            'ed': {
                'values': [
                    'val',
                    'var',
                    True,
                ]
            }
        }
    }

    nested_empty_data = {
        'nest': {
            'ed': {
                'empty': {},
                'data': [],
            }
        }
    }

    flat_data = {
        'nest/ed/values/0': 'val',
        'nest/ed/values/1': 'var',
        'nest/ed/values/2': True,
    }

    flat_value = {
        'single/text': 'value',
    }

    assert utils.flatten(nested_data) == flat_data
    assert utils.flatten(nested_empty_data) == {}
    assert utils.flatten(flat_data) == flat_data
    assert utils.flatten(flat_value) == flat_value
