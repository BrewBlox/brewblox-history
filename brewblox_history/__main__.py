"""
Example of how to import and use the brewblox service
"""

import logging

from brewblox_service import events, service

from brewblox_history import influx

LOGGER = logging.getLogger(__name__)


def main():
    app = service.create()

    # Setup history functionality
    events.setup(app)
    influx.setup(app)

    # Add all default endpoints, add prefix, and announce service to gateway
    #
    # Default endpoints are:
    # {prefix}/api/doc (Swagger documentation of endpoints)
    # {prefix}/_service/status (Health check: this endpoint is called to check service status)
    #
    # The prefix is automatically added for all endpoints. You don't have to do anything for this.
    # To change the prefix, you can use the --name command line argument.
    #
    # See brewblox_service.service for more details on how arguments are parsed.
    #
    # The default value is "brewblox".
    # This means you can now access the example/endpoint as "/brewblox/example/endpoint"
    service.furnish(app)

    # service.run() will start serving clients async
    service.run(app)


if __name__ == '__main__':
    main()
