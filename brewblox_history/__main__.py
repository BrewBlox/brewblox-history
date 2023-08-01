"""
Example of how to import and use the brewblox service
"""


from brewblox_service import http, mqtt, scheduler, service

from brewblox_history import (datastore_api, error_response, redis, relays,
                              socket_closer, timeseries_api, victoria)
from brewblox_history.models import ServiceConfig


def create_parser():
    parser = service.create_parser('history')
    parser.add_argument('--ranges-interval',
                        help='Interval (sec) between updates in live ranges. [%(default)s]',
                        default=10,
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


def main():
    parser = create_parser()
    config = service.create_config(parser, model=ServiceConfig)
    app = service.create_app(config)

    async def setup():
        scheduler.setup(app)
        http.setup(app)
        mqtt.setup(app)
        socket_closer.setup(app)
        victoria.setup(app)
        timeseries_api.setup(app)
        redis.setup(app)
        datastore_api.setup(app)
        relays.setup(app)
        error_response.setup(app)

    service.run_app(app, setup())


if __name__ == '__main__':
    main()
