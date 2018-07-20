"""
Example of how to import and use the brewblox service
"""

from brewblox_service import brewblox_logger, events, scheduler, service

from brewblox_history import builder, influx, relays, sse

LOGGER = brewblox_logger(__name__)


def create_parser(default_name='history'):
    parser = service.create_parser(default_name=default_name)
    parser.add_argument('--broadcast-exchange',
                        help='Eventbus exchange to which device services broadcast their state. [%(default)s]',
                        default='brewcast')
    parser.add_argument('--logging-exchange',
                        help='Eventbus exchange to which device services broadcast their logs. [%(default)s]',
                        default='logcast')
    parser.add_argument('--skip-influx-config',
                        help='Skip configuring Influx database after each connection. '
                        'This is useful when using custom configuration.',
                        action='store_true')
    return parser


def main():
    app = service.create_app(parser=create_parser())

    scheduler.setup(app)
    events.setup(app)
    influx.setup(app)
    builder.setup(app)
    sse.setup(app)
    relays.setup(app)

    relays.get_data_relay(app).subscribe(
        exchange_name=app['config']['broadcast_exchange'],
        routing='#'
    )
    relays.get_log_relay(app).subscribe(
        exchange_name=app['config']['logging_exchange'],
        routing='#'
    )

    service.furnish(app)
    service.run(app)


if __name__ == '__main__':
    main()
