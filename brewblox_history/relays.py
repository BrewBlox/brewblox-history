"""
Writes data from subscribed events to the database.

After a listener is set, it will relay all incoming messages.

Messages are expected to conform to the following schema:

    {
        'key': str,
        'data': dict,
        'timestamp': int,  # optional: ms since the Unix epoch
    }

The `data` dict is flattened: the key for each field is a /-separated path to the nested value.
Only finite numbers are stored: numeric strings and bools are converted, other values are dropped.
See HistoryEvent for which names are refused, and when the timestamp is used.

If we'd received an event where:

    {
        'key': 'controller1',
        'data': {
            'block1': {
                'sensor1': {
                    'settings': {
                        'setting': 'setting',
                        'enabled': True
                    },
                    'values': {
                        'value': '12.5',
                        'other': 1
                    }
                }
            }
        }
    }

`data` would become:

    {
        'block1/sensor1/settings/enabled': 1.0,
        'block1/sensor1/values/other': 1.0,
        'block1/sensor1/values/value': 12.5
    }
"""

import logging

from pydantic import ValidationError

from . import mqtt, utils, victoria
from .models import HistoryEvent

LOGGER = logging.getLogger(__name__)


def setup() -> None:
    config = utils.get_config()
    mqtt_client = mqtt.CV.get()

    @mqtt_client.subscribe(config.history_topic + '/#')
    async def on_history_message(
        _client: object,
        topic: str,
        payload: bytes,
        _qos: int,
        _properties: dict,
    ) -> None:
        try:
            evt = HistoryEvent.model_validate_json(payload)
            await victoria.CV.get().write(evt)
            # Lazy formatting: str(evt.data) is costly, and debug logging is usually off
            LOGGER.debug('MQTT: %s = %.30s...', evt.key, evt.data)

        except ValidationError as ex:
            LOGGER.error(f'Invalid history event: {topic} {utils.strex(ex)}')
