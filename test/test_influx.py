"""
Tests brewblox_history.influx
"""

import pytest
from brewblox_history import influx


TESTED = influx.__name__


@pytest.fixture
async def app(app, mocker):
    mocker.patch(TESTED + '.events.get_listener')

    influx.setup(app)
    return app


async def test_query(app, client):
    # database offline
    args = {
        'database': 'brewblox',
        'query': 'select * from controller'
    }
    res = await client.post('/query', json=args)
    assert res.status == 500
