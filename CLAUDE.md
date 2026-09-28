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
pytest                                    # full suite, 100% branch coverage required
pytest --no-cov test/test_victoria.py     # one file; without --no-cov a partial run fails on coverage
pytest --no-cov test/test_victoria.py -k name
ruff format --check --diff                # formatting; `ruff format` fixes
ruff check                                # lint: all rules, minus the ignores in pyproject.toml (each with its reason)
pyright                                   # type check, standard mode ([tool.pyright]); what CI runs
invoke testclean                          # remove containers left by a killed pytest
invoke image                              # build the service image locally (tag `local`)
docker compose up                         # the service with hot reload, plus eventbus, redis and both databases
```

The gate before every commit, as in CI: `pytest`, `ruff format --check`, `ruff check`, `pyright`.
Code is clean when it is committed; a deliberate exception gets `# noqa: <rule>` or
`# pyright: ignore[<rule>]` with the reason.

Tests need Docker: pytest-docker starts the eventbus, redis, victoria, victoria-dense and
victoria-legacy (v1.129.1: the legacy database, the single one ctl ran before 0.12.0, a migration's source)
services from test/docker-compose.yml once per session. test/test_database.py runs the
client against the databases; series names must be unique per test, since the databases
live for the whole session. Its `now` fixture freezes history's clock: never move it past
real time, or the database replaces the points it sees within the latency offset (and,
below a step of 1m, every point after its own now). An export may hold a series in several
lines (`exported()` merges them), and stops at the database's now unless given an end. Every test has a 10s timeout (`--timeout`) that
also covers fixture setup, so on a machine without the images the first test errors while
`docker compose up` is still pulling: run `docker compose -f test/docker-compose.yml pull`
first (CI does). `asyncio.sleep` calls over 0.1s print the test name: config intervals in
tests are milliseconds, so a long sleep in a test means a real delay slipped through.

## Architecture

- Every feature module exposes `setup()`, which builds its singleton and stores it in a
  module-level `CV` ContextVar; consumers call `module.CV.get()`. `app_factory.create_app()`
  runs the `setup()` calls in dependency order (mqtt, redis, victoria, downsample, migrate,
  relays), and `lifespan()` enters the background features (mqtt, redis, victoria, migrate,
  downsample: migrate first, the downsampler reads where legacy samples end) in an
  AsyncExitStack.
  Config is `utils.get_config()`, an lru-cached `ServiceConfig` read from `BREWBLOX_HISTORY_*`
  env vars and `.appenv` (written by parse_appenv.py from the container's command-line args;
  test/test_parse_appenv.py keeps the two in sync: every field has an argument).
  `dense_retention` is read in VictoriaMetrics' `-retentionPeriod` format
  (models.parse_retention: a bare number counts months), since ctl gives both the same value.
- Databases: `victoria-dense` (`dense_*` settings) receives the raw samples, and `victoria`
  (`victoria_*`), the long-term one, holds only `sparse_interval` averages. There is no single-database
  mode: a stack without ctl adds the dense service as this repository's `docker-compose.yml` does. VictoriaClient
  keeps one httpx client per database: raw writes go to dense, `ping` checks both, `fields` returns the
  union (the long-term one alone if dense fails). The settings are validated as ctl validates what it renders;
  an unknown one (such as a leftover `dense_enabled`) is ignored.
- Downsampler (downsample.py): one task that every
  `downsample_interval` averages each `sparse_interval` ended at least `downsample_lag` ago
  from dense into the long-term database (`/api/v1/import`, stamped at the interval's end),
  in chunks of `downsample_chunk`. Every import carries the marker series `victoria.MARKER`
  at the new cursor (hidden from `fields`); the cursor advances only after a successful
  import (or, once a migration is planned, skips ahead to where the legacy samples end: see
  Migration), and is found again from the marker at startup and when the marker falls behind
  (the long-term database lost imports it held in memory): an hour before it, in whole intervals,
  since the marker can reach disk before rows of its own import (the same samples give the same
  averages; where dense lost samples, an average can come out partial, and after a `sparse_interval`
  change that hour holds both grids). Reads get the cursor (`VictoriaClient.cursor`)
  `SEARCHABLE_DELAY` later; `age` counts from the marker while that hour is averaged again. `VictoriaClient.dense_since` comes
  from the dense database itself, at startup and hourly: the first day with series (per-day
  index, no samples read), then its first hour with a sample. The task logs errors and
  goes on: the service also serves the datastore. `/timeseries/ping` reports
  `downsample_age`.
- Migration (migrate.py): a best-effort background job that moves the legacy database
  (`victoria-legacy`: the single database ctl ran before 0.12.0, renamed by ctl) into the new ones, started by ctl through
  `POST /timeseries/migrate` (GET status, DELETE cancel or `?discard=true`); once it is done, the user removes the
  legacy database with ctl, which never waits. The datastore (`brewblox-history`/`migration`) holds only what it
  started with and its outcome, each change saved before the job acts on it; its progress is in the databases,
  and it resumes at startup. Phases: seed (the last `dense_days` of raw samples, native export streamed into
  dense, a day at a time back from where legacy samples end), walk (`sparse_interval` averages of the whole legacy
  history into the long-term database, in chunks of `downsample_chunk`, newest first, pausing as long as each
  chunk took; each chunk's import carries its marker, `victoria.MIGRATION_MARKER` labelled with the job's id;
  repeated until each chunk has its marker or was tried twice, also when that raised: `lost_chunks`), done
  (`missing_series` lists legacy series without averages). A marker does not prove its chunk is on disk: a crash
  of the long-term database can keep it while losing rows (accepted: the migrated history is past brews; live
  capture is what must be reliable). Each run first checks the legacy database has no samples after the last one
  planning found (`legacy_last`; after a rollback): it stops, the reason in `last_error`. The downsampler never
  starts before `VictoriaClient.legacy_end`, waits until the migration state is read (`legacy_end_known`;
  also with a marker, for the hour before it, while reads count on the averages up to the marker; a document
  that is not a datastore value, or holds no integer `legacy_end`, counts as no boundary), and imports under `legacy_lock`, which setting `legacy_end` also
  takes: once a migration is planned it imports nothing at or before it.
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
  `select_timeframe` gives start, end (`now - query_latency` when open-ended, and at the
  latest: VM replaces points within its latency offset with copies) and
  `step = max(duration / query_desired_points, minimum_step, 1s)`; `plan_ranges` splits it
  into queries per database: a step below `sparse_interval` where dense has
  samples (its retention, or from `dense_since` if later) reads dense; otherwise the step
  rounds up to a multiple of `sparse_interval` on the epoch grid, the long-term database
  answers up to the downsampler's `cursor` (until known: `steady_cursor`, which lags by
  lag + tick + searchable delay) and dense the rest. Every start is on its step's grid (VM
  rounds too, from 50 points). `ranges` sends `avg_over_time` with exact-name `or`
  selectors (planner.quote escapes `\` and `"`; batches stay under VM's 16 KiB query limit)
  and `latency_offset=query_latency`. When dense fails, a plan with a long-term part answers
  with that part; a dense-only plan that fails or finds none of the fields is answered by the
  long-term database at `sparse_interval` (plan_fallback). A dense read outage is warned once,
  and its end logged. An answer that is mostly empty (the series with the most points got fewer than
  half of `query_desired_points`, and some series has a hole: planner.has_hole) is asked once more
  (plan_refinement): at the step scaled by that share, rounded up, over only where it has points and a step
  either side, at most `REFINE_MAX_POINTS` per series (VM refuses over 30000 requested, empty ones included),
  and only when that is finer; not after the fallback, nor from dense while dense reads fail. A series gets
  the finer points, or keeps its first ones up to the finer plan's last point (merge_refined); any failure of
  the finer query keeps the first answer. `csv` exports raw samples
  (dense from the first `sparse_interval` grid point where it has them, averages before) in
  windows of `csv_chunk_*`, drops rows at or before the last one, and fails if dense does.
  `fields` lists series. The WebSocket `/timeseries/stream` runs one task per command id:
  `ranges` sends the window once (`initial: true`, `VictoriaClient.initial_ranges`); while
  the query is open-ended (utils.is_open_ended), every `ranges_interval` it sends the points
  after the newest one sent (`follow_up_ranges`, planner.plan_follow_up), if there are any: at
  the frame's step capped at `follow_up_step_max` (at least `minimum_step`), from the
  database with the raw samples and never the fallback, with `nocache` (from 50 points VM
  rounds a cacheable query's start to its step's grid), at most `FOLLOW_UP_MAX_POINTS` (VM
  refuses over 30000 per series). A failed query or send is asked again from the same point;
  when the clock went back more than a minute, the stream starts over with an initial message
  listing every field (the UI clears only for one with ranges). When a dense batch
  fails, the dense part of a plan is left out for every field. `metrics` pushes the cache
  every `metrics_interval`.
- Datastore (datastore_api, redis): namespaced JSON documents in Redis; changes are
  published on `brewcast/datastore/<namespace>` so the UI and services can subscribe.
- Errors: the catch-all handler in app_factory returns `ErrorResponse` with status 500; a
  rejected database query raises `httpx.HTTPStatusError`, a transport error (unreachable,
  timeout) raises `ConnectionError` naming the database (victoria.named_errors), and a failed
  write is logged with the database and its reason, and swallowed. Dense failures in `ranges`
  and `fields` degrade to the long-term database instead of raising (see Read path), and a failing
  refinement keeps the first answer.

## Testing rules

- The `app` fixture must stay synchronous: contextvars set in async fixtures are invisible
  to the test function.
- The shared `client` fixture uses httpx's `ASGITransport`. WebSocket tests build their own
  client with `httpx_ws.ASGIWebSocketTransport` inside the test: it holds an anyio cancel
  scope that must be entered and exited in the same task, and pytest-asyncio runs fixture
  setup and teardown in different tasks.
- Tests build models with the declared types (the type checker does not see pydantic's
  conversions): raw input, such as a string duration or extra fields, goes through
  `model_validate`. Settings are built with `TestConfig` (test/conftest.py): it takes only the
  values given, not the environment or `.appenv` (`ServiceConfig` reads both, also in
  `model_validate`).
- pytest-httpx responses are single-use; mark a response `is_reusable=True` when the code
  under test requests it more than once.
- Do not loosen or skip a failing assertion to get green; the code is the suspect first.

## Git

- Commit messages carry no AI attribution trailers (no Co-Authored-By, no session links).
- Discuss before building when a decision is Elco's to make; review findings are reported
  before anything is fixed; commits happen only when he asks for that commit.
- PRs target `develop` in BrewBlox/brewblox-history. CI runs `uv run pytest`, then
  `ruff format --check`, `ruff check` and `pyright` (the locked versions), then builds the
  image for amd64, arm/v7 and arm64. Python stays on 3.11 and arm/v7 stays supported
  because of wheel availability on the Pi.
- docs/ is the repository's documentation (tracked): docs/design.md is the design reference, updated in
  the same change as the code it describes. In-flight plans (the working design log is
  sessions/plan/plan-dense-sparse-history.md), review records and handoffs go under sessions/ (gitignored).
