# History Service

The history service is the gatekeeper for Brewblox databases. It writes data from history events, and offers REST interfaces for querying the InfluxDB and Redis databases.

## Features

### QueryClient ([influx.py](./brewblox_history/influx.py))

Handles directly querying InfluxDB. API functions eventually call this.

### InfluxWriter ([influx.py](./brewblox_history/influx.py))

Periodically writes scheduled data points to InfluxDB.

Publicly offers the `write_soon()` function, where data can be scheduled for writing.

### DataRelay ([relays.py](./brewblox_history/relays.py))

Subscribes to the `--history-topic` topic on the event bus, and schedules all received data for writing to the database.

### Datastore ([redis.py](./brewblox_history/redis.py))

Offers a simple wrapper around the Redis API.
Changes are broadcast to the `--datastore-topic` topic.

## REST API

### queries ([query_api.py](./brewblox_history/query_api.py))

REST API for the InfluxDB database. Also offers WebSocket endpoints for streamed values.

### datastore ([datastore_api](./brewblox_history/datastore_api.py))

REST API for the Redis datastore.
