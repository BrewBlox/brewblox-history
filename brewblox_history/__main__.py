"""
Example of how to import and use the brewblox service
"""

from brewblox_service import brewblox_logger, events, service

from brewblox_history import builder, influx

LOGGER = brewblox_logger(__name__)


def create_parser(default_name='history'):
    parser = service.create_parser(default_name=default_name)
    parser.add_argument('--broadcast-exchange',
                        help='Eventbus exchange to which device services broadcast their state. [%(default)s]',
                        default='brewcast')
    return parser


def main():
    app = service.create_app(parser=create_parser())

    # Setup history functionality
    events.setup(app)
    influx.setup(app)
    builder.setup(app)

    influx.get_relay(app).subscribe(
        exchange_name=app['config']['broadcast_exchange'],
        routing='#'
    )

    service.furnish(app)
    service.run(app)


if __name__ == '__main__':
    main()
