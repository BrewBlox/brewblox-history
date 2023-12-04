import logging
from contextlib import AsyncExitStack, asynccontextmanager
from pprint import pformat

from fastapi import FastAPI

from . import (datastore_api, mqtt, redis, relays, timeseries_api, utils,
               victoria)

LOGGER = logging.getLogger(__name__)


def init_logging(debug: bool):
    level = logging.DEBUG if debug else logging.INFO
    unimportant_level = logging.INFO if debug else logging.WARN
    format = '%(asctime)s.%(msecs)03d [%(levelname).1s:%(name)s:%(lineno)d] %(message)s'
    datefmt = '%Y/%m/%d %H:%M:%S'

    logging.basicConfig(level=level, format=format, datefmt=datefmt)
    logging.captureWarnings(True)

    logging.getLogger('gmqtt').setLevel(unimportant_level)
    logging.getLogger('httpx').setLevel(unimportant_level)
    logging.getLogger('httpcore').setLevel(logging.WARN)


@asynccontextmanager
async def lifespan(app: FastAPI):
    LOGGER.info(utils.get_config())
    LOGGER.debug('ROUTES:\n' + pformat(app.routes))
    # LOGGER.debug('LOGGERS:\n' + pformat(logging.root.manager.loggerDict))

    async with AsyncExitStack() as stack:
        await stack.enter_async_context(mqtt.lifespan())
        await stack.enter_async_context(redis.lifespan())
        yield


def create_app() -> FastAPI:
    config = utils.get_config()
    init_logging(config.debug)

    # Call setup functions for modules
    mqtt.setup()
    redis.setup()
    victoria.setup()
    relays.setup()

    # Create app
    # OpenApi endpoints are set to /api/doc for backwards compatibility
    prefix = f'/{config.name}'
    app = FastAPI(lifespan=lifespan,
                  docs_url=f'{prefix}/api/doc',
                  redoc_url=f'{prefix}/api/redoc',
                  openapi_url=f'{prefix}/openapi.json')

    # Include all endpoints declared by modules
    app.include_router(datastore_api.router, prefix=prefix)
    app.include_router(timeseries_api.router, prefix=prefix)

    return app
