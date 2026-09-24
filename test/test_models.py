"""
Tests brewblox_history.models
"""

import logging

import pytest
from pydantic import ValidationError

from brewblox_history import models


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

    assert models.flatten(nested_data) == flat_data
    assert models.flatten(nested_empty_data) == {}
    assert models.flatten(flat_data) == flat_data
    assert models.flatten(flat_value) == flat_value

    # Sorted by full path, across nesting levels
    assert list(models.flatten({'b': 1, 'a': {'d': 2, 'c': [3, 4]}, 'a/b': 5})) == [
        'a/b',
        'a/c/0',
        'a/c/1',
        'a/d',
        'b',
    ]


def test_config_ignores_unknown_settings():
    # Unknown settings must not prevent startup
    config = models.ServiceConfig(_env_file=None, unknown_setting='value')
    assert not hasattr(config, 'unknown_setting')


def test_history_event_data(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture):
    monkeypatch.setattr(models, '_refused_fields', set())
    data = {
        'nest': {'ed': {'values': ['val', 1, True, False, '8', None]}},
        'ok,=\\ ': 4,  # escaped when written
        # Only finite numbers are kept
        'nan': 'nan',
        'inf': float('inf'),
        'ninf': '-inf',
        'huge': 10**400,
        # Names the line protocol cannot express are refused
        '': 1,
        'new\nline': 2,
        'q"uote': 3,
        'q"text': 'text',  # not a number: ignored before its name matters
    }

    evt = models.HistoryEvent(key='k', data=data)
    assert evt.data == {
        'nest/ed/values/1': 1.0,
        'nest/ed/values/2': 1.0,
        'nest/ed/values/3': 0.0,
        'nest/ed/values/4': 8.0,
        'ok,=\\ ': 4.0,
    }

    # Each refused name is logged once
    models.HistoryEvent(key='k', data=data)
    assert [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING] == [
        f'Refused history field {name!r}: the database cannot store this name'
        for name in ['k/', 'k/new\nline', 'k/q"uote']
    ]


def test_history_event_data_json():
    # Values only JSON carries, on the path relays uses
    huge = '1' + '0' * 400
    evt = models.HistoryEvent.model_validate_json(
        '{"key": "k", "data": {"nan": NaN, "inf": Infinity, "ninf": -Infinity, '
        f'"exp": 1e400, "huge": {huge}, "text": "8", "ok": 1.5, "flag": false}}}}'
    )
    assert evt.data == {'flag': 0.0, 'ok': 1.5, 'text': 8.0}


@pytest.mark.parametrize('key', ['', '#comment', 'new\nline'])
def test_history_event_key_refused(key: str, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture):
    monkeypatch.setattr(models, '_refused_fields', set())
    with pytest.raises(ValidationError):
        models.HistoryEvent(key=key, data={'a': 1, 'q"uote': 2})

    # The event is refused as a whole: no separate refusal of its fields
    assert models._refused_fields == set()
    assert not [r for r in caplog.records if r.levelno == logging.WARNING]


def test_history_event_key():
    # Escaped when written
    evt = models.HistoryEvent(key='my "spark",1\\ =#', data={})
    assert evt.key == 'my "spark",1\\ =#'


@pytest.mark.parametrize(
    'value, expected',
    [
        (None, None),
        (1_700_000_000_000, 1_700_000_000_000),
        (1_700_000_000_000.6, 1_700_000_000_001),
        (-5, -5),  # left to the tolerance check
        ('1700000000000', None),
        ('2023-11-14T22:13:20Z', None),
        (True, None),
        (float('nan'), None),
        (float('inf'), None),
        (10**400, None),
        ({}, None),
    ],
)
def test_history_event_timestamp(value, expected):
    # An unusable timestamp is ignored: it must not cost the publisher its event
    evt = models.HistoryEvent(key='k', data={}, timestamp=value)
    assert evt.timestamp == expected


@pytest.mark.parametrize(
    'field, expected',
    [
        ('', None),
        (', "timestamp": 12', 12),
        (', "timestamp": "now"', None),
        (', "timestamp": 1' + '0' * 400, None),
    ],
)
def test_history_event_timestamp_json(field: str, expected):
    evt = models.HistoryEvent.model_validate_json(f'{{"key": "k", "data": {{}}{field}}}')
    assert evt.timestamp == expected
