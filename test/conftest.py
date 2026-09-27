"""
Master file for pytest fixtures.
Any fixtures declared here are available to all test functions in this directory.
"""

import asyncio
import logging
from collections.abc import AsyncGenerator
from pathlib import Path

import pytest
from asgi_lifespan import LifespanManager
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource
from pytest_docker.plugin import Services as DockerServices

from brewblox_history import app_factory, utils
from brewblox_history.models import DatastoreValue, ServiceConfig

LOGGER = logging.getLogger(__name__)


class TestConfig(ServiceConfig):
    """
    An override for ServiceConfig that only uses
    settings provided to __init__()

    This makes tests independent from env values
    and the content of .appenv
    """

    # Not a test class, although test modules import it
    __test__ = False

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (init_settings,)


class FakeDatastore:
    """The datastore (brewblox_history.redis), with the documents as JSON, as Redis holds them.
    The next `failures` calls raise."""

    def __init__(self) -> None:
        self.docs: dict[tuple[str, str], str] = {}
        self.failures = 0

    async def _call(self) -> None:
        # Other tasks run while Redis answers
        await asyncio.sleep(0)
        if self.failures:
            self.failures -= 1
            raise ConnectionError('datastore down')

    async def get(self, namespace: str, doc_id: str) -> DatastoreValue | None:
        await self._call()
        raw = self.docs.get((namespace, doc_id))
        return None if raw is None else DatastoreValue.model_validate_json(raw)

    async def set(self, value: DatastoreValue) -> DatastoreValue:
        await self._call()
        self.docs[(value.namespace, value.id)] = value.model_dump_json()
        return value

    async def delete(self, namespace: str, doc_id: str) -> int:
        await self._call()
        return 1 if self.docs.pop((namespace, doc_id), None) else 0


@pytest.fixture(scope='session')
def docker_compose_file():
    return Path('./test/docker-compose.yml').resolve()


@pytest.fixture(autouse=True)
def config(
    monkeypatch: pytest.MonkeyPatch,
    docker_services: DockerServices,
) -> ServiceConfig:
    cfg = TestConfig(
        debug=True,
        mqtt_host='localhost',
        mqtt_port=docker_services.port_for('eventbus', 1883),
        redis_host='localhost',
        redis_port=docker_services.port_for('redis', 6379),
        victoria_host='localhost',
        victoria_port=docker_services.port_for('victoria', 8428),
        dense_host='localhost',
        dense_port=docker_services.port_for('victoria-dense', 8428),
    )
    monkeypatch.setattr(utils, 'get_config', lambda: cfg)
    return cfg


@pytest.fixture
def legacy_url(docker_services: DockerServices) -> str:
    """The legacy database: a migration's source."""
    return f'http://localhost:{docker_services.port_for("victoria-legacy", 8428)}/victoria-legacy'


@pytest.fixture(autouse=True)
def m_sleep(monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest) -> None:
    """
    Allows keeping track of calls to asyncio sleep.
    For tests, we want to reduce all sleep durations.
    Set a breakpoint in the wrapper to track all calls.
    """
    real_func = asyncio.sleep

    async def wrapper(delay: float, *args: object, **kwargs: object) -> object:
        if delay > 0.1:
            # Shown in the test output: a long sleep means a real delay slipped into a test
            print(f'asyncio.sleep({delay}) in {request.node.name}')  # noqa: T201
        return await real_func(delay, *args, **kwargs)

    monkeypatch.setattr('asyncio.sleep', wrapper)


@pytest.fixture(autouse=True)
def setup_logging(config):
    app_factory.setup_logging(debug=True)


@pytest.fixture(autouse=True)
def reset_duplicate_filters():
    """DuplicateFilter remembers the last message: without a reset,
    a test's first message is dropped if the previous test ended with it."""
    loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]
    for flt in [f for logger in loggers for f in logger.filters if isinstance(f, utils.DuplicateFilter)]:
        flt.__dict__.pop('last_log', None)


@pytest.fixture
def app() -> FastAPI:
    """
    Override this in test modules to bootstrap required dependencies.

    IMPORTANT: This must NOT be an async fixture.
    Contextvars assigned in async fixtures are invisible to test functions.
    """
    return FastAPI()


@pytest.fixture
async def manager(app: FastAPI) -> AsyncGenerator[LifespanManager, None]:
    """
    AsyncClient does not automatically send ASGI lifespan events to the app
    https://asgi.readthedocs.io/en/latest/specs/lifespan.html

    For testing, this ensures that lifespan() functions are handled.
    If you don't need to make HTTP requests, you can use the manager
    without the `client` fixture.
    """
    async with LifespanManager(app) as mgr:
        yield mgr


@pytest.fixture
async def client(app: FastAPI, manager: LifespanManager) -> AsyncGenerator[AsyncClient, None]:
    """
    The default test client for making REST API calls.
    Using this fixture will also guarantee that lifespan startup has happened.
    """
    # AsyncClient does not automatically send ASGI lifespan events to the app
    # https://asgi.readthedocs.io/en/latest/specs/lifespan.html
    #
    # WebSocket tests build their own client with httpx_ws.ASGIWebSocketTransport
    # inside the test: that transport holds an anyio cancel scope, which must be
    # entered and exited in the same task, and pytest-asyncio runs fixture setup
    # and teardown in different tasks.
    async with AsyncClient(base_url='http://test', transport=ASGITransport(app=app)) as ac:
        yield ac
