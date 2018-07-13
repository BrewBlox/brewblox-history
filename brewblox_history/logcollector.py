"""
Subscribes to the BrewBlox logging exchange, and persists messages to InfluxDB
"""


from functools import partial

from aiohttp import web
from brewblox_history import influx
from brewblox_service import brewblox_logger, events

LOGGER = brewblox_logger(__name__)


def setup(app: web.Application):
    writer = influx.InfluxWriter(
        app,
        database='brewblox_logs',
        downsampling=False
    )
    events.get_listener(app).subscribe(
        exchange_name='brewblox-service-logs',
        routing='#',
        on_message=partial(_collect, writer)
    )


async def _collect(writer, sub, key, message):
    print(writer, sub, key, message, sep='\n')
