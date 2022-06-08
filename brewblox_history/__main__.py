"""
Example of how to import and use the brewblox service
"""

import json
import traceback

from aiohttp import web
from brewblox_service import (brewblox_logger, http, mqtt, scheduler, service,
                              strex)

from brewblox_history import (datastore_api, redis, relays, socket_closer,
                              timeseries_api, victoria)

LOGGER = brewblox_logger(__name__)


def create_parser(default_name='history'):
    parser = service.create_parser(default_name=default_name)
    parser.add_argument('--write-interval',
                        help='Interval (sec) between writing batches of received data to Influx. [%(default)s]',
                        default=30,
                        type=float)
    parser.add_argument('--ranges-interval',
                        help='Interval (sec) between updates in live ranges. [%(default)s]',
                        default=30,
                        type=float)
    parser.add_argument('--metrics-interval',
                        help='Interval (sec) between updates in live metrics. [%(default)s]',
                        default=5,
                        type=float)
    parser.add_argument('--redis-url',
                        help='URL for the Redis database',
                        default='redis://redis')
    parser.add_argument('--victoria-url',
                        help='URL for the Victoria Metrics database',
                        default='http://victoria:8428/victoria')
    parser.add_argument('--datastore-topic',
                        help='Synchronization topic for datastore updates',
                        default='brewcast/datastore')
    parser.add_argument('--minimum-step',
                        help='Minimum period (sec) for range data downsampling',
                        default=10,
                        type=float)
    return parser


@web.middleware
async def controller_error_middleware(request: web.Request, handler: web.RequestHandler) -> web.Response:
    try:
        return await handler(request)

    except web.HTTPError:  # pragma: no cover
        raise

    except Exception as ex:
        app = request.app
        message = strex(ex)
        debug = app['config']['debug']
        LOGGER.error(f'[{request.url}] => {message}', exc_info=debug)

        response = {
            'error': message,
            'traceback': traceback.format_tb(ex.__traceback__),
        }

        return web.HTTPInternalServerError(text=json.dumps(response),
                                           content_type='application/json')


def main():
    app = service.create_app(parser=create_parser())

    scheduler.setup(app)
    http.setup(app)
    mqtt.setup(app)
    socket_closer.setup(app)
    victoria.setup(app)
    timeseries_api.setup(app)
    redis.setup(app)
    datastore_api.setup(app)
    relays.setup(app)

    app.middlewares.append(controller_error_middleware)

    service.furnish(app)
    service.run(app)


if __name__ == '__main__':
    main()
