"""
Tests brewblox_history.models
"""

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
