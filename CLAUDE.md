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

Tests need Docker: pytest-docker starts the eventbus, redis and victoria services from
test/docker-compose.yml once per session. Every test has a 10s timeout (`--timeout`) that
also covers fixture setup, so on a machine without the images the first test errors while
`docker compose up` is still pulling: run `docker compose -f test/docker-compose.yml pull`
first (CI does). `asyncio.sleep` calls over 0.1s print the test name: config intervals in
tests are milliseconds, so a long sleep in a test means a real delay slipped through.

## Architecture

- Every feature module exposes `setup()`, which builds its singleton and stores it in a
  module-level `CV` ContextVar; consumers call `module.CV.get()`. `app_factory.create_app()`
  runs the `setup()` calls in dependency order (mqtt, redis, victoria, relays), and
  `lifespan()` enters the background features (mqtt, redis, victoria) in an AsyncExitStack.
  Config is `utils.get_config()`, an lru-cached `ServiceConfig` read from `BREWBLOX_HISTORY_*`
  env vars and `.appenv` (written by parse_appenv.py from the container's command-line args;
  test/test_parse_appenv.py keeps the two in sync).
- Write path: MQTT `brewcast/history/#` -> relays.on_history_message -> `HistoryEvent`,
  which sanitizes at ingest: models.flatten turns the nested `data` dict into `/`-separated
  field paths, only finite numbers are kept, and names the line protocol cannot express are
  refused (a field named empty, or with a newline or `"`, is dropped and logged once; a key
  that is empty, starts with `#` or holds a newline invalidates the event) ->
  victoria.write, one Influx line-protocol POST per event to `/write?precision=ms`
  (`<service> field=value,... [ms]`). Names are escaped, backslash first: VM rejects a line
  with a raw `,` or a trailing `\`, stores a raw `=` in a field key as a wrong series, and
  unescapes `\\`. The event's optional `timestamp` (int ms) is written when within 10 s of
  our clock, else the database stamps arrival. VictoriaMetrics runs with
  `-influxMeasurementFieldSeparator=/`, so series are named `<service>/<field/path>`.
  write() also fills the in-memory cache that `metrics()` serves; that endpoint never
  queries the database.
- Read path (timeseries_api): `ranges` issues one `avg_over_time` query_range per field
  with `step = max(duration / query_desired_points, minimum_step)` (utils.select_timeframe);
  `csv` streams `/api/v1/export` and transposes per timestamp; `fields` lists series. The
  WebSocket `/timeseries/stream` runs one task per command id: `ranges` sends the window
  once (`initial: true`), then re-queries from `start = now()` every `ranges_interval` while
  the query is open-ended (utils.is_open_ended); `metrics` pushes the cache every
  `metrics_interval`.
- Datastore (datastore_api, redis): namespaced JSON documents in Redis; changes are
  published on `brewcast/datastore/<namespace>` so the UI and services can subscribe.
- Errors: the catch-all handler in app_factory returns `ErrorResponse` with status 500; a
  rejected database query raises `httpx.HTTPStatusError`, a rejected write is logged with
  the database's reason and swallowed.

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
