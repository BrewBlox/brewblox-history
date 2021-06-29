"""
Example of how to import and use the brewblox service
"""

from aiohttp import web
from brewblox_service import (brewblox_logger, http, mqtt, scheduler, service,
                              strex)

from brewblox_history import (datastore_api, history_api, influx, redis,
                              relays, tsdb_api, utils, victoria)

LOGGER = brewblox_logger(__name__)


def create_parser(default_name='history'):
    parser = service.create_parser(default_name=default_name)
    parser.add_argument('--write-interval',
                        help='Interval (sec) between writing batches of received data to Influx. [%(default)s]',
                        default=5,
                        type=float)
    parser.add_argument('--poll-interval',
                        help='Interval (sec) between queries in live SSE requests. [%(default)s]',
                        default=5,
                        type=float)
    parser.add_argument('--influx-host',
                        help='Influx database host',
                        default='influx')
    parser.add_argument('--redis-url',
                        help='URL for the Redis database',
                        default='redis://redis')
    parser.add_argument('--victoria-host',
                        help='Victoria Metrics database host',
                        default='victoria')
    parser.add_argument('--victoria-opentsdb-port',
                        help='Victoria Metrics OpenTSDB port',
                        type=int,
                        default=4242)
    parser.add_argument('--victoria-prometheus-port',
                        help='Victoria Metrics Prometheus port',
                        type=int,
                        default=8428)
    parser.add_argument('--datastore-topic',
                        help='Synchronization topic for datastore updates',
                        default='brewcast/datastore')
    return parser


@web.middleware
async def controller_error_middleware(request: web.Request, handler: web.RequestHandler) -> web.Response:
    try:
        return await handler(request)
    except Exception as ex:
        LOGGER.error(f'REST error: {strex(ex)}', exc_info=request.app['config']['debug'])
        return web.json_response({'error': strex(ex)}, status=500)


def main():
    app = service.create_app(parser=create_parser())

    scheduler.setup(app)
    http.setup(app)
    mqtt.setup(app)
    utils.setup(app)
    influx.setup(app)
    victoria.setup(app)
    tsdb_api.setup(app)
    history_api.setup(app)
    redis.setup(app)
    datastore_api.setup(app)
    relays.setup(app)

    app.middlewares.append(controller_error_middleware)

    service.furnish(app)
    service.run(app)


if __name__ == '__main__':
    main()
