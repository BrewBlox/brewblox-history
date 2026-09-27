# History service design

This document describes the history service as built, and why. It changes with the code; where the two disagree, the code is right.

## 1. Overview

History writes the history events from the eventbus into VictoriaMetrics, serves the timeseries API (REST and a WebSocket stream) for the UI's graphs, and fronts the Redis datastore. `victoria-dense` receives every raw sample and keeps it for 30 days; `victoria`, the long-term database, holds only 60 s averages that history computes from dense. Before brewblox-ctl 0.12.0, ctl ran one database that kept every sample for 100 years; with the Spark publishing every second (devcon #639, firmware #685, 2026-09-24) it would grow fast, long graphs would scan millions of raw samples, and its default 5 s flush writes gigabytes a day to the SD card. ctl 0.12.0 renames it `victoria-legacy`, the legacy database, which history migrates into the new ones and the user then removes.

| Service | Data | Image | Retention | Holds |
|---|---|---|---|---|
| `victoria` (long-term) | `./victoria`, created empty by ctl 0.12.0 | v1.152.0 | 100y | `sparse_interval` (60 s) averages, and the markers |
| `victoria-dense` | `./victoria-dense` | v1.152.0 | 30d | every raw sample, plus up to 30 days of legacy raw samples |
| `victoria-legacy` | the old `./victoria`, renamed | v1.129.1 | 100y | nothing new: the migration's source |

## 2. Databases

- **Two instances**, because open-source single-node VictoriaMetrics cannot downsample or set retention per series: `-downsampling.period`, `-retentionFilter` and multi-tenancy are Enterprise or cluster features; Enterprise downsampling keeps the last value, not an average; stream aggregation cannot forward. So history averages, with no vmagent or vmalert (checked against the v1.152.0 source and docs, 2026-09-20).
- **A fresh long-term database**, not the old directory reused: it holds only averages (uniform meaning and CSV density); 5 s legacy data gets 12x fewer samples, and the averages take 3-13% of the disk of the legacy data they come from (measured); the old data skips v1.133's index migration and stays a byte-identical restore point.
- **The legacy database** runs while `./victoria-legacy` exists, on the admin port only. History never reads it for graphs: a period from before the upgrade is empty until the migration reaches it, or for good if the migration is declined.
- **Names.** `victoria` keeps meaning long-term (route, directory, `victoria_*` settings), since `/victoria` is a documented Grafana datasource; Grafana there sees 60 s averages, plus the markers `brewblox-history/downsampled` and `brewblox-history/migrated{migration=<job>}` that `fields` hides. Dense is on `/victoria-dense`. With `-influxMeasurementFieldSeparator=/`, series are named `<service>/<field/path>`.
- **Versions.** v1.129.1 and v1.152.0 agree on everything history uses. The legacy database stays on v1.129.1: v1.133 migrates each partition's index once (minutes of slow start, no clean downgrade). Images exist for amd64, arm64 and arm/v7.
- **Dense retention 30 days:** a brew three weeks old still renders at 1 s. With monthly partitions, 30d keeps up to ~61 days on disk, against ~44 for 14d.
- **Long-term resolution 60 s**, ctl's `victoria.sparse_interval`: fine enough for every window that does not read dense (from ~16.7 h, a step of at least 60 s), and 60 times fewer samples than 1 s. Choose it before migrating: after a change, older averages keep their grid and alias at steps that are not a multiple of both, and an unfinished migration stops. The dense resolution is each Spark's `--broadcast-interval`.
- **Dedup at 1 ms.** For two samples at one timestamp VictoriaMetrics keeps the biggest value, not the latest: a re-import is a no-op, so every interval is averaged once, after it is final. The rewind, retries and the seed's inclusive day boundaries write a timestamp twice; without dedup a resumed seed day was stored twice (tested).
- **Flush every 10 min long-term, every 5 min dense.** SD writes follow the number of flushes, mostly per-part metadata; 5 min trades them against what a power cut loses (see Measurements). At 60 s the long-term database would make 1440 parts a day, one per import.

## 3. Write path

MQTT `brewcast/history/#` -> `relays.on_history_message` -> `HistoryEvent` -> `victoria.write`: one Influx line-protocol POST per event to dense's `/write?precision=ms`.

- **Sanitized at ingest.** `data` is flattened into `/`-separated fields. Only finite numbers are kept: bools and numeric strings go through `float()`; the rest, NaN and inf are dropped (a bare `NaN` broke the UI's `JSON.parse`). Data that is not a dict is refused.
- **Escaping:** `\` first, then `,` and space, and `=` in field keys. On v1.152.0 a raw `,` in a block name made the database reject the line (a Spark lost all its history while the name existed), a raw `=` stored a wrong series with value 0, and a trailing `\` was a 400. The UI forbids these; the REST API does not.
- **Refused names.** A field named empty or with a newline or `"` is dropped and logged once; a key that is empty, starts with `#` or holds a newline invalidates the event; a series name over 1 KiB drops the field (or, from a long key, the event). None can be escaped, measured: an empty field key or a newline is a 400, a leading `#` a comment (204, nothing stored), an odd number of `"` silent corruption (204, that field 0, the rest of the line lost), and `\"` keeps the backslash. Renaming would not help: the UI builds series names itself, and two names could merge. The UI's block-name rule produces none of these.
- **The event timestamp.** devcon sends an optional `timestamp`, int ms UTC, the tick's deadline (ticks at x.500). History writes it when within 10 s of its clock, 10 s included (`TIMESTAMP_TOLERANCE`); otherwise the database stamps arrival, with one warning per key. An unusable timestamp (string, bool, NaN, inf, an int beyond float) is ignored and the event written. Arrival stamps put a sample where its read arrived, and every 5th tick is a slower full read: some 1 s buckets end up empty, others double. Samples can land in the past, so `downsample_lag` (30 s) must exceed the delivery delay, which devcon's 5 s `broadcast_timeout` bounds; a lag under `TIMESTAMP_TOLERANCE` + `SEARCHABLE_DELAY` = 16 s is refused.
- **One write per event**; no batching until MQTT backpressure is measured (since v1.105 one bad line fails a whole batch). A failure is logged with the database and its reason, and swallowed. After a write failed on a database error (unreachable, or a 5xx), the first successful one logs `<url>: writes work again`; a rejected line (a 4xx) is a problem with its data, not an outage.
- **Every value, every tick.** Measured on v1.152.0 for 200 series with deadline stamps: every second 10.0 MB/day, changed-only 10.3, changed with a 5 s floor 10.8. Dropping unchanged points would bias `avg_over_time`, a sample mean.
- **Metrics** come from a cache of `(value, time)` tuples that `write()` fills; they never query the database. A real 120-field event takes 349 µs before the POST, against 696 µs before the cheaper cache, sort and escaping (the previous history: ~750 µs; x86, 2026-09-24), with a byte-identical line.

## 4. Read path

`timeseries_api` -> `victoria` -> `planner`, pure functions in integer Unix seconds.

- **Step** = max(duration // 1000 (`query_desired_points`), `minimum_step`, 1 s): 1 s below 2000 s windows, 3 s for 1h, 14 s for 4h. Dense serves steps below 60 s (windows up to ~16.7 h, derived). 1d gives 120 s, 3d 300 s, 7d 660 s.
- **Routing** (`plan_ranges`). A step below `sparse_interval` starting inside the dense horizon (now - `dense_retention` + `dense_margin`, or `dense_since` if later) reads dense alone. Otherwise the step rounds up to a multiple of `sparse_interval`; the long-term database answers up to the last grid point at or before the downsampler's cursor, dense the rest. Until the cursor is known the planner assumes now - (lag + tick + `SEARCHABLE_DELAY`). The long-term lag (~1-2 min) is never visible.
- **Every start is on its step's grid.** Averaging 60 s averages at an arbitrary step aliases: 1d's 86 s windows alternately cover one and two minute-samples (simulated: ~20% amplitude error on a 20-minute fridge cycle). The database aligns only from 50 points.
- **Cost:** the worst dense scan is 60k samples per field; a 6-month graph of 20 fields reads ~5M samples instead of ~316M.
- **No copies.** Below a 1m step VictoriaMetrics replaces points within its latency offset of now with a copy of an older one. Ranges end at now - `query_latency` (5 s) at the latest, explicit ends included, and pass `latency_offset=query_latency`. The container's `-search.latencyOffset` does not matter.
- **Selectors:** exact-name `or` filters, form-encoded, at most 100 names and 8 KiB per batch (the database refuses queries over 16 KiB). `planner.quote` escapes `\` and `"`. `avg_over_time(...) keep_metric_names` keeps `__name__`, so both databases answer the same queries.
- **Dense failing degrades** (never in follow-ups): a dense-only plan that fails or finds none of the fields is answered by the long-term database; otherwise the long-term part answers, and a failed dense batch drops the dense part for every field. The outage is warned once, its end logged.
- **The long-term database failing does not degrade:** plans with a long-term part, the fallback, CSV before the dense horizon and `fields` fail (a 500 on REST, an aborted CSV transfer, a logged error on the stream, tried again). Dense-only reads, follow-ups, writes and metrics go on.
- **CSV exports raw samples**: dense from the first grid point in its horizon, averages before, in windows of 6h or 7d, dropping rows at or before the last one sent. Dense failing fails it. It streams a window at a time, so memory stays bounded (the previous history buffered the whole export: a day of 5 fields at 1 Hz peaked at 42 MB); the 200 goes first, and a failure aborts the transfer part-way.
- **`fields`** is the union of both databases, markers hidden, or the long-term one alone when dense fails. **`ping`** checks both, reports each failing one, and adds `downsample_age`.
- **Errors:** a transport error (unreachable, timeout) raises a `ConnectionError` naming the database; a rejected query raises `httpx.HTTPStatusError`; the catch-all handler answers 500.

## 5. Live stream

The WebSocket `/timeseries/stream` runs one task per command id. The previous stream re-queried from `start=now()` without an end. Since VictoriaMetrics replaces the newest points with copies, that worked only because `minimum_step`, `ranges_interval` and `-search.latencyOffset` were all 10 s, and each point still arrived twice; with a 1 s poll it goes empty.

- **Initial, then follow-ups:** the window once (`initial: true`), then, while open-ended, every `ranges_interval` (1 s), the points after the newest one sent, up to now - `query_latency`, only when there are any. Each point arrives once, with its real value; a closed window sends one message.
- **Grid:** follow-ups continue where the initial query's last bucket ended, so a long-term answer continues with dense points. Step = max(`minimum_step`, min(frame step, `follow_up_step_max`), 1 s): 1 s for 10m, 3 s for 1h, 10 s from about 3h. The frame's step would advance a 3d graph every 5 min.
- **Source:** dense, never the fallback, with `nocache` (from 50 points the database rounds a cacheable start to its step's grid). At most 1000 points per follow-up: the database refuses more than 30000 per series, which stuck a stream for good after an 8.3 h outage (reproduced).
- **Retries and clock steps:** the stream moves on only after the query answered and the send succeeded. When the clock goes back more than 60 s, the stream starts over with an initial message listing every field (`values: []` where empty, so the UI drops what it held); a smaller step back waits.
- **`query_latency` 5 s** covers the measured ~1.3 s delivery plus ~3.2 s write-to-searchable. A line ends 5-6 s before now.
- **`metrics`** pushes the cache every `metrics_interval` (1 s).

## 6. Downsampler

Every `downsample_interval` (15 s), one task averages each `sparse_interval` (60 s) that ended at least `downsample_lag` (30 s) ago from dense into the long-term database (`/api/v1/import`, chunks of `downsample_chunk`, 1h), stamped at the interval's end: one import a minute while keeping up.

- **Query:** `avg_over_time` on dense with `nocache` and `latency_offset=downsample_lag`; non-finite averages dropped (the database skips a whole JSON line with `Infinity` and still answers 204); empty windows stay gaps.
- **Marker:** every import carries `brewblox-history/downsampled` at the new cursor, which advances only after a 2xx; reads get it `SEARCHABLE_DELAY` (6 s) later. At startup the cursor comes from the marker (no local state). With none, a whole-index lookup (`start=1`, since `start=0` means the last day) tells an expired marker (warned) from a fresh start; either way averaging starts at the latest of dense's first sample, the dense retention's start and the legacy end.
- **Lost imports:** each tick `check_archive` compares the marker with the newest searchable cursor; behind means the long-term database lost imports it held in memory, and the cursor is found again.
- **The one-hour rewind:** wherever the cursor is found from the marker, it becomes max(marker - ceil(3600 / interval) x interval, start). An import reaches disk in parts, so the marker can be on disk before its rows, and a power loss would leave a hole in averages that outlive dense. An hour, because a 10-minute flush is expected to lose at most ~20 min (an expectation, not a proven bound).
- **Migration boundary:** discovery averages nothing until the migration state is read, also with a marker (a rewind could otherwise reach before an unknown boundary); meanwhile reads, the lag and `check_archive` use the marker. A document that is not a datastore value, or has no integer `legacy_end`, is no boundary.
- **Fence:** imports run under `legacy_lock`, which setting `legacy_end` also takes: once a migration is planned, nothing at or before its boundary is imported, and the cursor skips ahead.
- **`dense_since`** comes from dense, at startup and hourly: the first day with series (per-day index, no samples read), then its first hour with a sample. While dense does not answer, each tick fails alike, so the failure is logged once, and it is asked again next tick with the last known value kept; after a restart the task still reads the marker, so reads and the age count on the averages up to it.
- **Visible failure:** a broken downsampler is silent on a dashboard, so a warning is logged once while the averages lag more than `downsample_max_lag` (10m), and ping reports `downsample_age`: seconds since the averages end (at the marker during the rewind), counted from the task's start until known. A hole is permanent only if unnoticed longer than the dense retention.
- **Downtime:** the cursor waits, then catches up from dense. Errors are logged and the task goes on: the service also serves the datastore.
- **Chunk size:** `json.loads` holds the GIL, so a parse blocks the event loop: an hour of 200 series takes ~6 ms on x86 (est. ~80 ms on a Pi 3), a 6h chunk ~20-45 ms (est. 0.3-0.6 s).

## 7. Migration

A background job moves the legacy database into the new ones. It is best effort: the migrated history is past brews, nice to have but not critical, so rare edge cases are accepted rather than handled with more machinery. Live capture through both new databases is what must be reliable. ctl never waits for the job. A migration that cannot run, a job that never ends or floods Redis or MQTT, and the service or its datastore going down stay severe.

### API

- **`POST /timeseries/migrate {source_url, earliest, dense_days=30}`** plans a new migration or resumes one, starts the job and returns its status. Planning, inside the POST: a per-day index lookup back to the first day with samples (usually today), then one `tlast`. While legacy is unreachable, the POST is a 500 naming it.
- **409** when running or done, after a `sparse_interval` change, for another `source_url` or `dense_days` on an unfinished migration, for an `earliest` after now, or on an invalid state. **Its `detail` is a contract:** ctl matches `The migration is running` verbatim; change it only with ctl.
- **`GET`** returns the state plus `running`, `chunks_done` (null until counted) and `last_error`, or null. **`DELETE`** cancels (a POST resumes); `?discard=true` removes the state: the only way past a job stopped by the rollback check (one stopped by a `sparse_interval` change also resumes once the interval is set back). Start and cancel share a lock.

### State

`brewblox-history`/`migration` holds what the job started with and its outcome: `phase` (seed, walk, done), `cancelled`, `job`, `source_url`, `earliest`, `dense_days`, `sparse_interval`, `chunk`, `legacy_last`, `legacy_end`, `chunks_total`, `started`, `finished`, `lost_chunks`, `missing_series`. Times are Unix seconds, `chunk` seconds, `lost_chunks` chunk end times. The stored `earliest` is where the walk starts, an interval before the one holding the POSTed time; a resume keeps it (ctl may send a later one). `legacy_end` ends the interval holding the last legacy sample (`legacy_last`). `dense_days` 0 skips the seed. Each transition is saved before the job acts on it; progress lives in the databases, so there are no per-chunk Redis writes.

### Phases

- **Seed:** the last `dense_days` of raw samples, a day at a time back from `legacy_end`, native export streamed into dense's native import; `dense_since` follows.
- **Walk:** hour chunks, newest first, so recent history appears within minutes. Each: `avg_over_time` on legacy (1 ms latency offset), one import with the marker `brewblox-history/migrated{migration=<job>}`, then a pause as long as the chunk took, so live traffic goes first.
- **Reconciliation:** the walk counts this job's markers and redoes chunks without one until each has one or was tried twice (a try that raised counts). The rest go into `lost_chunks`; `missing_series` lists legacy series without averages. `done` means the tries are over, not that every average is on disk.

### Recovery

- **Rollback check:** each run first checks legacy has no samples after `legacy_last`; if it was written again (the legacy setup after a rollback), the job stops with `last_error`. Without it, a later update resumed with the old end, and the cleanup would delete the newer data (reproduced).
- **Resume at startup:** before the downsampler, so `legacy_end` is known; a separate task under the job lock, leaving a job a request started. Not resumed: cancelled, done, invalid (its `legacy_end` still fences the downsampler) or after a `sparse_interval` change.
- **Errors** are retried with a doubling pause (1-60 s). The walk's per-minute averages are correct over the interim 1 s data in the legacy database.

### Progress and durability

Progress lives only in the markers: one per chunk, in the same import as its averages, labelled with the job, so a discard followed by a new start redoes the work. There are no snapshots, checkpoints, audit or verify endpoint. A snapshot is not a barrier (its final flush skips parts in a merge), so snapshots did not prove that a chunk is on disk, and a marker cannot prove it either; an audit against the legacy database would take a second full read and a wait, for history that is best effort. The upgrade seam is documented rather than fixed, and the downsampler recovers its own averages with the one-hour rewind; their limits are under Accepted limits.

## 8. Configuration

Settings come from `BREWBLOX_HISTORY_*` variables and `.appenv`, which `parse_appenv.py` writes from the container's arguments.

| Setting | Default |
|---|---|
| `victoria_protocol`, `_host`, `_port`, `_path`, `_timeout` | `http`, `victoria`, `8428`, `/victoria`, `60s` |
| `dense_protocol`, `_host`, `_port`, `_path` | `http`, `victoria-dense`, `8428`, `/victoria-dense` |
| `dense_retention`, `dense_margin` | `30d`, `1h` |
| `sparse_interval` | `60s` |
| `downsample_lag`, `_interval`, `_chunk`, `_max_lag` | `30s`, `15s`, `1h`, `10m` |
| `minimum_step`, `query_desired_points`, `query_duration_default` | `1s`, `1000`, `1d` |
| `query_latency`, `ranges_interval`, `follow_up_step_max`, `metrics_interval` | `5s`, `1s`, `10s`, `1s` |
| `csv_chunk_dense`, `csv_chunk_sparse` | `6h`, `7d` |

- **Validation:** `query_latency`, the CSV chunks, `follow_up_step_max`, `minimum_step`, `sparse_interval` and the downsampler's interval, chunk and max lag positive; `minimum_step` and `sparse_interval` whole seconds and the latter a multiple of the former; `follow_up_step_max` <= `sparse_interval`; `dense_retention` >= 1d; 0 <= `dense_margin` < `dense_retention`; `downsample_lag` >= 16 s. ctl checks what it renders the same way, so a bad `brewblox.yml` fails in ctl instead of crash-looping history, which also serves the datastore. The downsampler's chunk and lag warning adapt to `sparse_interval` instead of refusing it: the chunk is used in whole intervals, at least one, and the warning waits at least `downsample_lag` + `sparse_interval` + `downsample_interval`, so it does not come and go while keeping up.
- **Unknown settings are ignored**, such as a leftover `dense_enabled` in the environment or `.appenv`; an unknown argument is warned about.
- **`dense_retention` is parsed like `-retentionPeriod`**, since ctl renders one string to both: a bare number or `M` counts months of 31 days (checked against v1.152.0 and metricsql v0.87.4).
- **Image:** Python 3.11 and arm/v7 stay for the Pi's wheels; uvloop stays at 0.21.0 (no armv7l wheel for 0.22).

## 9. What history relies on from brewblox-ctl

- **They ship together, in both directions.** There is no mode without the dense database: this history without `victoria-dense` loses every sample, and the legacy history image under ctl 0.12.0's template writes raw samples into the long-term database. A stack without ctl adds `victoria-dense` as this repository's `docker-compose.yml` does.
- **Settings:** `BREWBLOX_HISTORY_DENSE_RETENTION`, `_SPARSE_INTERVAL`, `_MINIMUM_STEP` (defaults `30d`, `60s`, `1s`), checked as history checks them.
- **Flags:** both new databases get `-dedup.minScrapeInterval=1ms` and `-influxMeasurementFieldSeparator=/`; `-inmemoryDataFlushInterval` is 10m on `victoria`, 5m on `victoria-dense`. Every VictoriaMetrics service gets `stop_grace_period: 30s` (a graceful stop writes memory to disk, measured 0.1 s).
- **Rename:** stop, `mv ./victoria ./victoria-legacy`, create `./victoria` and `./victoria-dense`; render `victoria-legacy` (v1.129.1, admin port only) while its directory exists.
- **Discard first:** the update discards any migration state before it offers one (a stale `done` would let the cleanup delete legacy with nothing migrated); if that keeps failing, it neither starts nor offers the migration.
- **Start:** a POST through the admin port with `source_url=http://victoria-legacy:8428/victoria-legacy`. `earliest` is the legacy database's first monthly partition, clamped to now - `victoria.retention` + 1 day (older imports are dropped and would count as lost). ctl retries 5xx and transport errors (30 x 10 s, 300 s timeout), and treats the 409 `The migration is running` as started.
- **Cleanup:** ctl never waits for the job. The user runs `brewblox-ctl database remove-legacy-history`, which refuses unless the phase is `done` (or `--force`, which also discards an unfinished migration), shows `lost_chunks` and `missing_series`, asks, deletes `./victoria-legacy` and restarts: the only irreversible step.
- **Rollback before the cleanup** is manual: discard the migration state (`DELETE /timeseries/migrate?discard=true`, or delete the datastore document `brewblox-history`/`migration`), stop, delete `./victoria` and `./victoria-dense`, move `./victoria-legacy` back, return to the previous ctl and to a history image from before the dense setup (this one loses every sample without `victoria-dense`), start. Without the discard, a later update's migration stops with `last_error`.
- **No raw samples from elsewhere:** ctl 0.12.0 removes `database from-influxdb` (InfluxDB was replaced in the 2021/08/02 release; the command would write raw samples into the long-term database).
- **Callers:** besides the UI, ctl uses `/timeseries/migrate` and ping's `downsample_age`; Grafana reads the databases directly.

## 10. Accepted limits

- **Power cut:** no write-ahead log, so up to twice the flush interval is lost: up to 10 min of raw samples in dense (typically 5), and up to 20 min of averages, which the rewind recomputes where dense still has the samples.
- **The one-hour rewind:** where dense lost samples too (the same power loss, or its own crash with history restarting within the hour), the replay averages what is left, and a partial average can replace a correct one, since dedup keeps the larger (samples 0 and 100 average 50; without the 0, 100 wins). After a `sparse_interval` change the replayed hour holds both grids. Not covered: the long-term database crashing alone while history runs, its marker surviving, or a crash during a long catch-up. Nothing orders writes between the databases, but dense flushes twice as often, so the replay is usually exact.
- **Dense down after discovery:** reads hold at the marker when a restart finds dense down, but when discovery runs while dense is down (within the hour after a successful look at where dense starts, or after the long-term database lost imports during the outage), the read cursor is an hour before the marker, and graphs of about 16.7 h and longer miss that hour's averages until dense answers. Accepted: it needs two failures at once, heals when dense answers, loses no data, and a general fix would reset the read cursor after every failed replay.
- **Reads before the migration state is read:** while the datastore is down at startup, reads trust the long-term database up to the marker, so a hole before it shows until the hour is averaged again.
- **Migration durability:** a long-term crash during the migration, or after `done` before its imports reach disk, can keep a marker while losing rows, unnoticed; those averages are gone once legacy is removed. A dense crash can lose the last seeded day; a restart during the seed seeds again.
- **The upgrade seam:** history accepts timestamps up to 10 s before arrival, and its downsampler starts before the migration, so new samples can share the intervals ending in (`legacy_end` - (ceil(10 s / `sparse_interval`) + 1) x `sparse_interval`, `legacy_end`] with legacy ones: two at every `sparse_interval` ctl allows (at least 10 s). There an average can be biased (dedup keeps the larger partial one), or, for a field only the new samples have, absent once dense expires. This holds if ctl stops the previous history service first (it stamps arrival), dense is fresh and the clock does not step back.
- **Late samples:** a follow-up that passed a bucket never sends a sample that arrives late in it; it shows on reload. `query_latency` rests on x86 figures.
- **Clock running ahead:** samples stay in the future; the downsampler reads its future marker as lost (a false warning), averages again from an hour before the newest marker before now, and waits.
- **Clock skew:** follow-ups assume the database's clock is not behind history's (true on one host). The dense outage flag is shared by all streams, so one failing query among others flips it each pass.
- **Resolution:** windows older than 30 days come at 60 s; after the cleanup, data from before the upgrade is 60 s averages only. The 9 `skip_changed` Spark fields arrive every 5 s.
- **`lost_chunks`** mixes expired chunks (ctl's clamp avoids them) and real losses.

## 11. Measurements

Real-world figures come from one reference system: a production brewery running the legacy setup on a VM on a NAS, logging 331 series at 1 s. Synthetic loads are used where two setups had to be compared under the same load.

### SD-card writes

The write volume that wears an SD card follows the number of flushes, not the amount of data. VictoriaMetrics has no write-ahead log: rows sit in memory, and every `-inmemoryDataFlushInterval` they are written as a new part, a directory of small files, each fsynced, with `parts.json` rewritten. Most of what reaches the disk is that per-part overhead: whole 4 KB blocks for files of a few hundred bytes, the metadata files, directory entries and an ext4 journal commit for every fsync. The samples themselves are a small share. The flush interval therefore changes the writes, not the size on disk: the same rows end up in the same merged parts.

The legacy database runs v1.129.1 with no flush flag, so its default of 5 s applies. Measured on 2026-09-27 on x86, each database on its own loop-mounted ext4, the same synthetic load for both (200 series at 1 Hz), bytes counted at the block device including the journal, over one hour:

| database | flush | written per day | into data files |
|---|---|---|---|
| legacy, as ctl ran it before 0.12.0 (v1.129.1) | 5 s | 6.3 GB (~19,300 parts at ~330 KB each) | ~4% |
| dense (v1.152.0) | 5 min | 163 MB | about a third |

The reference brewery's legacy database (its own counters over 3.25 days) makes about 14,200 parts a day and writes 165 MB a day into data files: about 4.6 GB a day at that per-part cost. Earlier runs of the same method over 3 hours gave, for dense: 60 s 634, 2 min 369, 5 min 169, 10 min 93-116 and 20 min 47 MB a day, with memory around 60-65 MB at every interval. The dense setup flushes dense every 5 minutes and the long-term database every 10 minutes (about 35 MB a day, estimated: it imports once a minute), so the stack writes about 0.2 GB a day instead of about 5-6 GB, some 25-30 times less. 5 minutes for dense is the middle ground: a power cut loses up to twice the interval of raw samples (typically one), and 10 minutes would save another 0.05-0.07 GB a day for twice that gap. Card life is not measured: at an assumed 1-3 TB of host writes for a 32 GB consumer card under this fsync-heavy pattern, the legacy rate matches the 1-2 year card life the documentation reports, and the dense setup's puts it beyond the card's other failure modes.

### Disk size per sample

Synthetic (2026-09-24): ~1.7 B with arrival-jittered timestamps, ~0.6 B jitter-free. The reference brewery (2026-09-26): values 0.214 B, 0.118 with devcon's lossless rounding; regular timestamps ~0, arrival stamps ~1.1 B. For 200 series at 1 Hz that is ~4 MB/day, ~0.23 GB for dense's 61 days on disk (derived, not measured). The 60 s averages cost 1.5 B each, 3% of the 1 s legacy data they come from; for noisy 5 s analog data 3.1 B, 13%.

### Timestamps and delivery delay

At the reference brewery, 2026-09-24 (1 s ticks, history stamping arrival, 5-minute runs):

- **1 s buckets:** arrival stamps left 1.1% empty and 0.4% double (`sparkey`), 0.1% and 0% (`valves`); with event stamps the only empties are skipped ticks (0.3-0.7%). Simulated: 0.7-5% empty with arrival stamps, up to ~25% for an unlucky phase; 0.02% with the deadline stamp.
- **Delivery delay** (database arrival minus event timestamp, p50/p95/p99/max ms): `sparkey` 39/188/237/1279 on CHANGED ticks, 71/219/292/383 on full ticks; `valves` 20/61/183/315 and 51/90/114/141. 1 of 597 was over 500 ms.

### Write to searchable

x86, unloaded v1.152.0 (2026-09-24): 2.05-3.16 s (median 3.05 s), 5.3 s for a new series. `SEARCHABLE_DELAY` is 6 s. Not yet measured on a Pi.

### Persistence probes

Probed on a throwaway v1.152.0 with a 10m flush (2026-09-25 and 2026-09-26): after `/internal/force_flush` a SIGKILL loses the data; after `/snapshot/create` it survives; a graceful `docker stop` writes memory to disk in 0.1 s; 6 of 60 imports were searchable in parts; a chunk across a month's start lost its older half while its end marker survived (SIGKILL); a snapshot took 0.13 s at 1 monthly partition, 2.65 s at 61. From the source, not reproduced: imports are parsed in 64 KiB blocks into shards with their own flush deadlines; a snapshot's final flush skips parts in a merge (0 of 80 probes showed it); a failing snapshot makes the database exit.

### Load, memory and migration time

- **Pi 3 load** (estimates from x86 measurements on a dev stack with two simulated Sparks, scaled ~13x): ingest 2-3% of a core per Spark, each database ~2%, each open 1 s graph ~2% (0.4% from 3h), the downsampler ~0.1%. devcon's CHANGED broadcaster costs more: ~13-15% of a core per Spark.
- **Memory on a 1 GB Pi 3** (estimates): the stack before ctl 0.12.0 430-480 MB; the second database ~60 MB; the legacy one ~100-150 MB while it answers the migration. That fits in ~930 MB usable at 96-128 MB `VM_memory_allowedBytes` per database.
- **Interim 1 s data in the legacy database** (estimates): an all-history query reaches `maxSamplesPerSeries` (30M) after 347 days of 1 Hz data, or 128 days on top of 3 years of 5 s data, and a multi-month graph of several fields risks an OOM on a 1 GB Pi 3 after ~2-3 months.
- **Seed format:** native export against JSON lines, 4.32M samples: 12.8 against 125 MB, ~3x faster import on x86. **Chunks:** a day of 180 series was ~4.5 MB of JSON, ~250 ms of parsing on x86 (est. ~3-4 s on a Pi 3): hence hour chunks.
- **Migration time, estimated** (ctl's estimate): on a Pi 3, ~7 s of work per legacy day of 5 s data and ~17 s of 1 s data, doubled by the pause, plus ~20 min of seed. Five years of 5 s data: ~7 h on a Pi 3, ~3 h on a Pi 4.

## 12. Rejected alternatives

- **vmagent stream aggregation:** aggregates by ingestion time, drops the first and last interval on each restart, cannot catch up, and is a fourth always-on process. **vmalert:** overwrites `__name__`, another container, no automatic replay.
- **`-dedup.minScrapeInterval=60s` with `force_merge`:** would irreversibly turn legacy 5 s data into the last value per minute.
- **Reusing `./victoria` in place:** raw and averages mix, legacy stays slow, the index migration runs, no clean way back. **Renaming it `./victoria-archive`:** breaks the Grafana docs and user overrides.
- **tmpfs or `DISABLE_FSYNC_FOR_TESTING` for dense:** 0.8-1.6 GB of RAM, or corruption on a power cut.
- **A 2 s Spark interval on small hosts:** 1 s everywhere until a Pi 3 measurement says otherwise.
- **An in-memory window for follow-ups** (lines ~1.5-2.5 s behind): a higher `query_latency` gives the same completeness without a module and a setting; it can come later without an API change.
- **Cursor discovery from the data**, and **a `dense_since` flag in Redis** (it blocked writes when it failed): replaced by the marker and the dense scan.
- **Stamping at receipt in history:** removes only history's jitter.
- **Only documenting the hole after a power loss** (instead of the one-hour rewind): live capture must be reliable.
- **Snapshots as checkpoints** (built, then removed): not a barrier, slower with more partitions, and a failing one exits the database. **Two markers per chunk:** replaced by the checkpoints, then by one marker per chunk. **A verify pass** before cleanup: dropped with the checkpoints, since a marker cannot prove its chunk is on disk and ctl never waits.
- **Row-counting markers, fingerprints, a full audit against legacy:** extra rows hide lost ones, values cannot be hashed exactly, and an audit means a second full read plus waiting.
- **"Stop after N empty windows"** instead of `earliest`: truncates history with a long gap. **An exact seam:** too much for two intervals per field, once.
