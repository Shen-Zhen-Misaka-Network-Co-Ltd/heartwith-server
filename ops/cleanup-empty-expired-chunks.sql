-- One-time, backed-up migration from row deletion to native chunk retention.
-- Run with psql in autocommit mode, never --single-transaction.
\set ON_ERROR_STOP on
SET lock_timeout = '3s';
SET statement_timeout = '30s';

SELECT floor(extract(epoch FROM now()) * 1000)::bigint - 86400000 AS cutoff_ms \gset

-- This tool only removes empty history. Nonempty expired data requires an
-- operator to verify its archive/rollups before using a different migration.
SELECT EXISTS (SELECT 1 FROM public.heart_rate_samples WHERE t_ms < :cutoff_ms) AS has_expired_rows \gset
\if :has_expired_rows
    \echo 'Refusing cleanup: expired raw rows still exist; verify archive and rollups first.'
    \quit 3
\endif

-- Each generated statement commits separately and holds at most 32 chunks.
-- Rerunning after a timeout is safe: already dropped chunks are not selected.
SELECT format(
    'SELECT count(*) AS removed_chunks FROM drop_chunks(''public.heart_rate_samples''::regclass, older_than => %s::bigint);',
    max(range_end_integer) + 1
)
FROM (
    SELECT range_end_integer,
           (row_number() OVER (ORDER BY range_end_integer) - 1) / 32 AS batch
    FROM timescaledb_information.chunks
    WHERE hypertable_schema = 'public'
      AND hypertable_name = 'heart_rate_samples'
      AND range_end_integer <= :cutoff_ms
) AS expired
GROUP BY batch
ORDER BY batch
\gexec

SELECT count(*) AS remaining_chunks
FROM timescaledb_information.chunks
WHERE hypertable_schema = 'public' AND hypertable_name = 'heart_rate_samples';
