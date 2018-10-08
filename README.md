# History Service

The history service is the gatekeeper for the InfluxDB database. It writes broadcasted data, and offers a REST interface for querying the database.

## Features

### QueryClient ([influx.py](./brewblox_history/influx.py))

Handles directly querying InfluxDB. API functions eventually call this.

### InfluxWriter ([influx.py](./brewblox_history/influx.py))

Periodically writes scheduled data points to InfluxDB.

Publicly offers the `write_soon()` function, where data can be scheduled for writing.

When connecting to InfluxDB, it will configure the database for automatic downsampling.

### DataRelay ([relays.py](./brewblox_history/relays.py))

Subscribes to the broadcast exchange on the event bus, and schedules all received data for writing to the database.

By default, it is subscribed to the broadcast exchange specified with the `--broadcast-exchange` commandline argument. Additional subscriptions can be added.

### LogRelay ([relays.py](./brewblox_history/relays.py))

Operates under the same principles as `DataRelay` (subscribe to events, write to database).

It is different in that it uses a different data protocol, and the log database is not downsampled.

## REST API

### subscribe ([influx.py](./brewblox_history/influx.py))

Adds another broadcast subscription. All data received with this subscription is written to InfluxDB.

### queries ([sse.py](./brewblox_history/sse.py))

Public query API for external clients. Input is sanitized before being passed on to InfluxDB.

### sse ([sse.py](./brewblox_history/sse.py))

Subscribe to regular updates of database values. The endpoint arguments are comparable to the ones used to get values from `queries`. It will periodically yield newly received data as SSE data.
