import logging
from contextlib import AsyncExitStack, asynccontextmanager
from pprint import pformat

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from . import (datastore_api, mqtt, redis, relays, timeseries_api, utils,
               victoria)
from .models import ErrorResponse

LOGGER = logging.getLogger(__name__)


def setup_logging(debug: bool):
    level = logging.DEBUG if debug else logging.INFO
    unimportant_level = logging.INFO if debug else logging.WARN
    format = '%(asctime)s.%(msecs)03d [%(levelname).1s:%(name)s:%(lineno)d] %(message)s'
    datefmt = '%Y/%m/%d %H:%M:%S'

    logging.basicConfig(level=level, format=format, datefmt=datefmt)
    logging.captureWarnings(True)

    logging.getLogger('gmqtt').setLevel(unimportant_level)
    logging.getLogger('httpx').setLevel(unimportant_level)
    logging.getLogger('httpcore').setLevel(logging.WARN)
    logging.getLogger('uvicorn.access').setLevel(unimportant_level)
    logging.getLogger('uvicorn.error').disabled = True


def add_exception_handlers(app: FastAPI):
    config = utils.get_config()
    logger = logging.getLogger('history.error')

    @app.exception_handler(Exception)
    async def catchall_handler(request: Request, exc: Exception) -> JSONResponse:
        short = utils.strex(exc)
        details = utils.strex(exc, tb=config.debug)
        content = ErrorResponse(error=str(exc),
                                details=details)

        logger.error(f'[{request.url}] => {short}')
        logger.debug(details)
        return JSONResponse(content.model_dump(), status_code=500)


@asynccontextmanager
async def lifespan(app: FastAPI):
    LOGGER.info(utils.get_config())
    LOGGER.debug('ROUTES:\n' + pformat(app.routes))
    LOGGER.debug('LOGGERS:\n' + pformat(logging.root.manager.loggerDict))

    async with AsyncExitStack() as stack:
        await stack.enter_async_context(mqtt.lifespan())
        await stack.enter_async_context(redis.lifespan())
        yield


def create_app() -> FastAPI:
    config = utils.get_config()
    setup_logging(config.debug)

    if config.debugger:  # pragma: no cover
        import faulthandler
        faulthandler.enable()

        import debugpy
        debugpy.listen(('0.0.0.0', 5678))
        LOGGER.info('Debugger is enabled and listening on 5678')

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

    # Set standardized error response
    add_exception_handlers(app)

    # Include all endpoints declared by modules
    app.include_router(datastore_api.router, prefix=prefix)
    app.include_router(timeseries_api.router, prefix=prefix)

    return app
