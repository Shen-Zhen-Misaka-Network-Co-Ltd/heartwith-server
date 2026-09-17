# Database

## Production Choice

Server deployments should use PostgreSQL with TimescaleDB.

Heart-rate data is a low-frequency time series. Writes arrive in small batches, while reads are dominated by:

- latest participant state for the lobby;
- recent samples by `collector_id` and time window;
- idempotency checks by `collector_id + seq`.

PostgreSQL gives mature constraints, transactions, indexing, and operational tooling. TimescaleDB adds hypertables for time-window storage and retention-friendly queries without changing the application API.

SQLite remains supported for local development and tests, but it is not the recommended server database.

## Configuration

Set `HEARTWITH_DATABASE_URL` before starting the Rust server:

```bash
export HEARTWITH_DATABASE_URL='postgres://heartwith:heartwith@127.0.0.1:5432/heartwith'
cargo run -p heartwith-server
```

If `HEARTWITH_DATABASE_URL` starts with `postgres://` or `postgresql://`, the server uses PostgreSQL. Other URLs continue to use SQLite, for example:

```bash
HEARTWITH_DATABASE_URL='sqlite://heartwith.db' cargo run -p heartwith-server
```

## Schema

- `collectors`: one logical collector per display name, token, device metadata, latest BPM.
- `collector_seqs`: recent idempotency window for `collector_id + seq`.
- `heart_rate_samples`: recent raw heart-rate samples with `(collector_id, t_ms)` indexes.
- `heart_rate_rollups`: hourly aggregates used to keep long-term storage bounded without preserving raw samples forever. Each bucket stores count, sum, sum of squares, min/max BPM, and first/last sample timestamps.

When TimescaleDB is installed, `heart_rate_samples` is converted to a hypertable on `t_ms` with 1-hour chunks. If the extension is unavailable, the server logs a warning and continues with plain PostgreSQL indexes.

## Retention

The server exposes the recent 24-hour raw chart window. TimescaleDB uses a native retention policy (`drop_after=86400000` milliseconds), running every 15 minutes with a two-minute runtime limit. It drops only fully expired one-hour chunks; physical storage may therefore retain up to roughly 25 hours plus scheduling delay. The API still returns at most 24 hours. PostgreSQL without a hypertable and SQLite use row expiry instead; SQLite also prunes during ingest.

At startup, missing hourly rollups are backfilled before the retention policy is configured. Existing rollups are never overwritten with potentially incomplete surviving raw rows. Runtime ingestion updates rollups in the same transaction as raw samples.

An ingest batch is acknowledged and published to the in-memory lobby only after its database transaction commits. On failure, its sequence remains retryable; heart-rate samples and sleep state must commit together.

Monitor TimescaleDB chunk count and PostgreSQL lock capacity. Row deletion does not remove empty chunks. Queries spanning thousands of chunks can exhaust the shared lock table even with few clients. In the 2026-09-17 incident, thousands of chunks exceeded the capacity configured with `max_locks_per_transaction=128` and `max_connections=25`. Capacity was raised to 1024 after backing up configuration and restarting PostgreSQL. Native retention now bounds chunk growth rather than relying on further capacity increases. See [PostgreSQL lock management](https://www.postgresql.org/docs/16/runtime-config-locks.html).

## Migrating Existing TimescaleDB Deployments

Before deploying native retention to a database with thousands of old chunks:

1. Take a full `pg_dump -Fc` backup and verify it can be listed with `pg_restore --list`.
2. Run `ops/cleanup-empty-expired-chunks.sql` with psql in autocommit mode, without `--single-transaction`. It refuses nonempty expired history, drops at most 32 old empty chunks per transaction, and stops on timeout. Never bypass this guard without verifying old data is archived/aggregated.
3. Deploy the server. It installs a millisecond integer-now function and one idempotent native retention job. A conflicting existing TTL fails configuration rather than silently changing the data retention contract.
4. Verify `timescaledb_information.jobs` and `job_stats`. Normal chunk count is about 25-26, possibly fewer with data gaps. The app's 15-minute sweeper warns when the job is absent, paused, failed, has not succeeded for over an hour, or chunk count exceeds 48. It no longer deletes raw rows on hypertables. Startup retention configuration uses a 3-second lock timeout and a 30-second statement timeout, including the initializer advisory lock.

The hourly rollup table remains separate and keeps its 90-day retention. Cleanup never cascades into it. Keep backups outside the database host; native expiry is not a backup system.

An ignored integration test exercises actual TimescaleDB, not SQLite emulation. Run only against a disposable test database:

```bash
HEARTWITH_RETENTION_TEST_URL=postgres://postgres:password@127.0.0.1:55439/heartwith_retention_test \
  cargo test -p heartwith-server timescale_retention_drops_chunks -- --ignored
```

Every accepted sample updates an hourly rollup row containing count, sum, sum of squares, min, max, and first/last timestamps. `sum_sq` lets analysis compute variance as `sum_sq / count - avg^2`. Rollups are kept for 90 days by default.

Series API queries read from the database with a time cutoff and `max_points` time buckets, so long ranges such as 6h and 24h do not require sending or rendering every raw sample.

## Example TimescaleDB

```bash
docker run --name heartwith-timescaledb \
  -e POSTGRES_DB=heartwith \
  -e POSTGRES_USER=heartwith \
  -e POSTGRES_PASSWORD=heartwith \
  -p 5432:5432 \
  -d timescale/timescaledb:latest-pg16
```
