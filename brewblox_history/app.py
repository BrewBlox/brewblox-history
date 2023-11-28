import logging
from contextlib import AsyncExitStack, asynccontextmanager
from pprint import pformat

from fastapi import FastAPI

from . import datastore_api, mqtt, redis, relays, timeseries_api, victoria
from .models import ServiceConfig

LOGGER = logging.getLogger(__name__)


def init_logging():
    config = ServiceConfig.cached()
    level = logging.DEBUG if config.debug else logging.INFO
    unimportant_level = logging.INFO if config.debug else logging.WARN
    format = '%(asctime)s.%(msecs)03d [%(levelname).1s:%(name)s:%(lineno)d] %(message)s'
    datefmt = '%Y/%m/%d %H:%M:%S'

    logging.basicConfig(level=level, format=format, datefmt=datefmt)
    logging.captureWarnings(True)

    logging.getLogger('gmqtt').setLevel(unimportant_level)
    logging.getLogger('httpx').setLevel(unimportant_level)
    logging.getLogger('httpcore').setLevel(logging.WARN)


def setup():
    mqtt.setup()
    redis.setup()
    victoria.setup()
    relays.setup()


@asynccontextmanager
async def lifespan(app: FastAPI):
    LOGGER.info(ServiceConfig.cached())
    LOGGER.debug('ROUTES:\n' + pformat(app.routes))
    # LOGGER.debug('LOGGERS:\n' + pformat(logging.root.manager.loggerDict))

    async with AsyncExitStack() as stack:
        await stack.enter_async_context(mqtt.lifespan())
        await stack.enter_async_context(redis.lifespan())
        yield


def create_app():
    init_logging()
    setup()

    config = ServiceConfig.cached()
    prefix = f'/{config.name}'
    app = FastAPI(lifespan=lifespan,
                  docs_url=f'{prefix}/api/doc',
                  redoc_url=f'{prefix}/api/redoc',
                  openapi_url=f'{prefix}/openapi.json')

    app.include_router(datastore_api.router, prefix=prefix)
    app.include_router(timeseries_api.router, prefix=prefix)

    return app
