"""
Master file for pytest fixtures.
Any fixtures declared here are available to all test functions in this directory.
"""


import logging
from pathlib import Path

import pytest
from fastapi import FastAPI
from httpx import AsyncClient
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource
from pytest_docker.plugin import Services as DockerServices

from brewblox_history import app_factory, models, utils

LOGGER = logging.getLogger(__name__)


class TestConfig(models.ServiceConfig):
    """
    An override for ServiceConfig that only uses
    settings provided to __init__()

    This makes tests independent from env values
    and the content of .appenv
    """

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


@pytest.fixture(scope='session')
def docker_compose_file():
    return Path('./test/docker-compose.yml').resolve()


@pytest.fixture(autouse=True)
def config(monkeypatch: pytest.MonkeyPatch, docker_services: DockerServices):
    cfg = TestConfig(
        debug=True,
        mqtt_host='localhost',
        mqtt_port=docker_services.port_for('mqtt', 1883),
        redis_host='localhost',
        redis_port=docker_services.port_for('redis', 6379),
    )
    monkeypatch.setattr(utils, 'get_config', lambda: cfg)
    yield cfg


@pytest.fixture(autouse=True)
def log_enabled(config):
    app_factory.init_logging(True)


@pytest.fixture
def app():
    """
    Override this in test modules to bootstrap the subset of required dependencies.
    This must NOT be an async fixture
    """
    app = FastAPI()
    return app


@pytest.fixture
async def lifespan(app):
    """
    Override this in test modules to manage module lifespan
    """
    yield


@pytest.fixture
async def client(config, app: FastAPI, lifespan):
    async with AsyncClient(app=app, base_url='http://test') as ac:
        yield ac
