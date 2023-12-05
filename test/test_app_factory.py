"""
Tests brewblox_history.app_factory
"""

import pytest
from httpx import AsyncClient

from brewblox_history import app_factory


@pytest.fixture
def app():
    return app_factory.create_app()


async def test_endpoints(client: AsyncClient, app):
    resp = await client.get('/history/api/doc')
    assert resp.status_code == 200

    resp = await client.get('/history/datastore/ping')
    assert resp.json() == {'ping': 'pong'}

    resp = await client.get('/history/timeseries/ping')
    assert resp.status_code == 200
    assert resp.json() == {'ping': 'pong'}
