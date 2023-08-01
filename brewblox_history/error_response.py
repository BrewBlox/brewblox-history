"""
Catches Python error, and returns appropriate error codes
"""

import json
import traceback

from aiohttp import web
from brewblox_service import brewblox_logger, strex

from brewblox_history.models import ServiceConfig

LOGGER = brewblox_logger(__name__)


@web.middleware
async def controller_error_middleware(request: web.Request, handler: web.RequestHandler) -> web.Response:
    try:
        return await handler(request)

    except web.HTTPError:  # pragma: no cover
        raise

    except Exception as ex:
        config: ServiceConfig = request.app['config']
        message = strex(ex)
        LOGGER.error(f'[{request.url}] => {message}', exc_info=config.debug)

        response = {
            'error': message,
            'traceback': traceback.format_tb(ex.__traceback__),
        }

        raise web.HTTPInternalServerError(text=json.dumps(response),
                                          content_type='application/json')


def setup(app: web.Application):
    app.middlewares.append(controller_error_middleware)
