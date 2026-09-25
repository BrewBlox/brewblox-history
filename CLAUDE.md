# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

The history service is the gatekeeper for the Brewblox databases: it writes history events
from the eventbus into VictoriaMetrics and serves the timeseries API (REST + WebSocket) the
UI graphs from, and it fronts the Redis datastore that holds UI and service settings.
This file holds what changes how an agent works here. `invoke --list` lists every task.

## Commands

The environment is uv (`uv sync`). VS Code terminals have `.venv/bin` on PATH; elsewhere
prefix commands with `uv run`.

```sh
pytest                                    # full suite: the gate, 100% branch coverage required
pytest --no-cov test/test_victoria.py     # one file; without --no-cov a partial run fails on coverage
pytest --no-cov test/test_victoria.py -k name
ruff format --check --diff                # what CI lints; `ruff format` fixes
ruff check                                # lint (select ALL, ignores in pyproject.toml); not enforced by CI
invoke testclean                          # remove containers left by a killed pytest
invoke image                              # build the service image locally (tag `local`)
docker compose up                         # the service with hot reload, plus eventbus, redis and victoria
```

Tests need Docker: pytest-docker starts the eventbus, redis, victoria and victoria-dense
services from test/docker-compose.yml once per session. test/test_database.py runs the
client against both databases; series names must be unique per test, since the databases
live for the whole session. Every test has a 10s timeout (`--timeout`) that
also covers fixture setup, so on a machine without the images the first test errors while
`docker compose up` is still pulling: run `docker compose -f test/docker-compose.yml pull`
first (CI does). `asyncio.sleep` calls over 0.1s print the test name: config intervals in
tests are milliseconds, so a long sleep in a test means a real delay slipped through.

## Architecture

- Every feature module exposes `setup()`, which builds its singleton and stores it in a
  module-level `CV` ContextVar; consumers call `module.CV.get()`. `app_factory.create_app()`
  runs the `setup()` calls in dependency order (mqtt, redis, victoria, downsample, relays),
  and `lifespan()` enters the background features (mqtt, redis, victoria, downsample) in an
  AsyncExitStack.
  Config is `utils.get_config()`, an lru-cached `ServiceConfig` read from `BREWBLOX_HISTORY_*`
  env vars and `.appenv` (written by parse_appenv.py from the container's command-line args;
  test/test_parse_appenv.py keeps the two in sync: every field has an argument).
  `dense_retention` is read in VictoriaMetrics' `-retentionPeriod` format
  (models.parse_retention: a bare number counts months), since ctl gives both the same value.
- Databases: `victoria` (`victoria_*` settings) is the long-term one. With `dense_enabled`
  (off by default; ctl turns it on) `victoria-dense` (`dense_*`) receives the raw samples,
  and the long-term one is meant to hold `sparse_interval` averages. VictoriaClient keeps
  one httpx client per database: raw writes go to dense when enabled, `ping` checks every
  database, `fields` returns the union (the long-term one alone if dense fails). Settings only the dense setup uses (and `minimum_step`,
  which predates it) are validated only with `dense_enabled`: without it the service, which
  also serves the datastore, must start whatever they are (ctl renders some either way).
- Downsampler (downsample.py, only with `dense_enabled`): one task that every
  `downsample_interval` averages each `sparse_interval` ended at least `downsample_lag` ago
  from dense into the long-term database (`/api/v1/import`, stamped at the interval's end),
  in chunks of `downsample_chunk`. Every import carries the marker series `victoria.MARKER`
  at the new cursor (hidden from `fields`); the cursor advances only after a successful
  import, is found again from the marker at startup, and is rewound when the marker falls
  behind (the long-term database lost imports it held in memory). Reads get the cursor
  (`VictoriaClient.cursor`) `SEARCHABLE_DELAY` later. `VictoriaClient.dense_since` comes
  from the dense database itself, at startup and hourly: the first day with series (per-day
  index, no samples read), then its first hour with a sample. The task logs errors and
  goes on: the service also serves the datastore. `/timeseries/ping` reports
  `downsample_age`.
- Write path: MQTT `brewcast/history/#` -> relays.on_history_message -> `HistoryEvent`,
  which sanitizes at ingest: models.flatten turns the nested `data` dict into `/`-separated
  field paths, only finite numbers are kept, and names the line protocol cannot express are
  refused (a field named empty, with a newline or `"`, or making a series name over 1 KiB is
  dropped and logged once; a key that is empty, starts with `#`, holds a newline or is over
  1 KiB invalidates the event) ->
  victoria.write, one Influx line-protocol POST per event to `/write?precision=ms`
  (`<service> field=value,... [ms]`). Names are escaped, backslash first: VM rejects a line
  with a raw `,` or a trailing `\`, stores a raw `=` in a field key as a wrong series, and
  unescapes `\\`. The event's optional `timestamp` (int ms) is written when within 10 s of
  our clock, else the database stamps arrival. VictoriaMetrics runs with
  `-influxMeasurementFieldSeparator=/`, so series are named `<service>/<field/path>`.
  write() also fills the in-memory cache that `metrics()` serves; that endpoint never
  queries the database.
- Read path (timeseries_api -> victoria -> planner, pure functions in integer Unix seconds):
  `select_timeframe` gives start, end (open-ended: `now - query_latency`) and
  `step = max(duration / query_desired_points, minimum_step, 1s)`; `plan_ranges` splits it
  into queries per database: with dense, a step below `sparse_interval` where dense has
  samples (its retention, or from `dense_since` if later) reads dense; otherwise the step
  rounds up to a multiple of `sparse_interval` on the epoch grid, the long-term database
  answers up to the downsampler's `cursor` (until known: `steady_cursor`, which lags by
  lag + tick + searchable delay) and dense the rest. Every start is on its step's grid (VM
  rounds too, from 50 points). `ranges` sends `avg_over_time` with exact-name `or`
  selectors (planner.quote escapes `\` and `"`; batches stay under VM's 16 KiB query limit)
  and `latency_offset=query_latency`. When dense fails, a plan with a long-term part answers
  with that part; a dense-only plan that fails or finds none of the fields is answered by the
  long-term database at `sparse_interval` (plan_fallback). A dense read outage is warned once,
  and its end logged. `csv` exports raw samples
  (dense from the first `sparse_interval` grid point where it has them, averages before) in
  windows of `csv_chunk_*`, drops rows at or before the last one, and fails if dense does.
  `fields` lists series. The WebSocket `/timeseries/stream` runs one task per command id:
  `ranges` sends the window once (`initial: true`), then re-queries from `start = now()`
  every `ranges_interval` while the query is open-ended (utils.is_open_ended); `metrics`
  pushes the cache every `metrics_interval`.
- Datastore (datastore_api, redis): namespaced JSON documents in Redis; changes are
  published on `brewcast/datastore/<namespace>` so the UI and services can subscribe.
- Errors: the catch-all handler in app_factory returns `ErrorResponse` with status 500; a
  rejected database query raises `httpx.HTTPStatusError`, a transport error (unreachable,
  timeout) raises `ConnectionError` naming the database (victoria.named_errors), and a failed
  write is logged with the database and its reason, and swallowed. Dense failures in `ranges`
  and `fields` degrade to the long-term database instead of raising (see Read path).

## Testing rules

- The `app` fixture must stay synchronous: contextvars set in async fixtures are invisible
  to the test function.
- The shared `client` fixture uses httpx's `ASGITransport`. WebSocket tests build their own
  client with `httpx_ws.ASGIWebSocketTransport` inside the test: it holds an anyio cancel
  scope that must be entered and exited in the same task, and pytest-asyncio runs fixture
  setup and teardown in different tasks.
- pytest-httpx responses are single-use; mark a response `is_reusable=True` when the code
  under test requests it more than once.
- Do not loosen or skip a failing assertion to get green; the code is the suspect first.

## Git

- Commit messages carry no AI attribution trailers (no Co-Authored-By, no session links).
- Discuss before building when a decision is Elco's to make; review findings are reported
  before anything is fixed; commits happen only when he asks for that commit.
- PRs target `develop` in BrewBlox/brewblox-history. CI runs `uv run pytest`, then
  `ruff format --check`, then builds the image for amd64, arm/v7 and arm64. Python stays
  on 3.11 and arm/v7 stays supported because of wheel availability on the Pi.
- Design work in progress is in docs/ (untracked until agreed); in-flight plans and review
  records go under sessions/ (gitignored).
