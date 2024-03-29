"""
Writes data from subscribed events to the database.

After a listener is set, it will relay all incoming messages.

Messages are expected to conform to the following schema:

    {
        'key': str,
        'data': dict,
    }

The `data` dict is flattened.
The key name for each field will be a /-separated path to the nested value.

If we'd received an event where:

    {
        'key': 'controller1',
        'data': {
            'block1': {
                'sensor1': {
                    settings: {
                        'setting': 'setting'
                    },
                    values: {
                        'value': 'val',
                        'other': 1
                    }
                }
            }
        }
    }

`data` would be flattened to:

    {
        'block1/sensor1/settings/setting': 'setting',
        'block1/sensor1/values/value': 'val',
        'block1/sensor1/values/other': 1
    }
"""

import logging

from pydantic import ValidationError

from . import mqtt, utils, victoria
from .models import HistoryEvent

LOGGER = logging.getLogger(__name__)


def setup():
    config = utils.get_config()
    mqtt_client = mqtt.CV.get()

    @mqtt_client.subscribe(config.history_topic + '/#')
    async def on_history_message(client, topic, payload, qos, properties):
        try:
            evt = HistoryEvent.model_validate_json(payload)
            await victoria.CV.get().write(evt)
            LOGGER.debug(f'MQTT: {evt.key} = {str(evt.data)[:30]}...')

        except ValidationError as ex:
            LOGGER.error(f'Invalid history event: {topic} {utils.strex(ex)}')
