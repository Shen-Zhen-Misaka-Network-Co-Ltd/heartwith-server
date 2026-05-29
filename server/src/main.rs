use std::{
    collections::{HashMap, HashSet, VecDeque},
    net::SocketAddr,
    path::PathBuf,
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use axum::{
    body::{to_bytes, Body},
    extract::{Path as AxumPath, Query, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse,
    },
    routing::{get, post},
    Json, Router,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sqlx::{
    postgres::PgPoolOptions,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    PgPool, Row, SqlitePool,
};
use tokio::sync::{broadcast, RwLock};
use tower::ServiceBuilder;
use tower_http::{cors::CorsLayer, services::ServeDir, set_header::SetResponseHeaderLayer};
use uuid::Uuid;

const MIN_BPM: i32 = 30;
const MAX_BPM: i32 = 240;
const ONLINE_AFTER_MS: i64 = 20_000;
const OFFLINE_AFTER_MS: i64 = 120_000;
const MAX_CLOCK_SKEW_MS: i64 = 60_000;
const MAX_SAMPLE_FUTURE_MS: i64 = 5_000;
const RAW_SAMPLE_TTL_MS: i64 = 24 * 60 * 60_000;
const MEMORY_SERIES_TTL_MS: i64 = 10 * 60_000;
const ROLLUP_TTL_MS: i64 = 90 * 24 * 60 * 60_000;
const ROLLUP_BUCKET_MS: i64 = 60 * 60_000;
const RETENTION_SWEEP_INTERVAL: Duration = Duration::from_secs(15 * 60);
const DEFAULT_SERIES_MAX_POINTS: i64 = 420;
const MAX_BODY_BYTES: usize = 128 * 1024;

#[derive(Clone)]
struct AppState {
    inner: Arc<RwLock<Store>>,
    events: broadcast::Sender<LobbyEvent>,
    db: Db,
}

#[derive(Default)]
struct Store {
    collectors: HashMap<String, Collector>,
    tokens: HashMap<String, String>,
    collector_tokens: HashMap<String, String>,
}

#[derive(Clone, Debug)]
struct Collector {
    collector_id: String,
    display_name: String,
    device_model: String,
    _client_platform: String,
    _app_version: String,
    _created_at_ms: i64,
    last_bpm: Option<i32>,
    last_seen_ms: Option<i64>,
    updated_at_ms: Option<i64>,
    seen_seqs: HashSet<u64>,
    seq_order: VecDeque<u64>,
    series: VecDeque<SeriesSample>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct UploadPolicy {
    batch_window_ms: u64,
    max_batch_window_ms: u64,
    low_power_batch_window_ms: u64,
    change_flush_bpm: u32,
    offline_cache_seconds: u64,
}

impl Default for UploadPolicy {
    fn default() -> Self {
        Self {
            batch_window_ms: 8_000,
            max_batch_window_ms: 8_000,
            low_power_batch_window_ms: 8_000,
            change_flush_bpm: 3,
            offline_cache_seconds: 300,
        }
    }
}

#[derive(Debug, Deserialize)]
struct SessionRequest {
    display_name: String,
    #[serde(default = "unknown")]
    device_model: String,
    #[serde(default = "unknown")]
    client_platform: String,
    #[serde(default = "zero_version")]
    app_version: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct SessionResponse {
    collector_id: String,
    collector_token: String,
    ingest_url: String,
    policy: UploadPolicy,
}

#[derive(Debug, Serialize, Deserialize)]
struct BatchPayload {
    schema: u8,
    collector_id: String,
    seq: u64,
    sent_at_ms: i64,
    #[serde(default)]
    display_name: Option<String>,
    #[serde(default)]
    device_model: Option<String>,
    samples: Vec<RelativeSample>,
    #[serde(default)]
    ble: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize)]
struct RelativeSample {
    dt_ms: i64,
    bpm: i32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SeriesSample {
    t_ms: i64,
    bpm: i32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Participant {
    collector_id: String,
    display_name: String,
    device_model: String,
    status: ParticipantStatus,
    last_bpm: Option<i32>,
    last_seen_ms: Option<i64>,
    updated_at_ms: Option<i64>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum ParticipantStatus {
    Online,
    Stale,
    Offline,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
enum LobbyEvent {
    #[serde(rename = "snapshot")]
    Snapshot {
        server_time_ms: i64,
        participants: Vec<Participant>,
    },
    #[serde(rename = "participant_update")]
    ParticipantUpdate { participant: Participant },
}

#[derive(Debug, Serialize, Deserialize)]
struct IngestResponse {
    ok: bool,
    accepted: usize,
    server_time_ms: i64,
    next_policy: Option<UploadPolicy>,
}

#[derive(Debug, Serialize, Deserialize)]
struct LobbyResponse {
    server_time_ms: i64,
    participants: Vec<Participant>,
}

#[derive(Debug, Deserialize)]
struct SeriesQuery {
    #[serde(default = "default_window")]
    window_seconds: i64,
    #[serde(default = "default_series_max_points")]
    max_points: i64,
}

#[derive(Debug, Serialize, Deserialize)]
struct SeriesResponse {
    collector_id: String,
    window_seconds: i64,
    samples: Vec<SeriesSample>,
}

#[derive(Clone)]
struct Db {
    pool: DbPool,
}

#[derive(Clone)]
enum DbPool {
    Postgres(PgPool),
    Sqlite(SqlitePool),
}

impl Db {
    async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let pool = if database_url.starts_with("postgres://")
            || database_url.starts_with("postgresql://")
        {
            DbPool::Postgres(
                PgPoolOptions::new()
                    .max_connections(10)
                    .connect(database_url)
                    .await?,
            )
        } else {
            let options = SqliteConnectOptions::from_str(database_url)?
                .create_if_missing(true)
                .foreign_keys(true);
            DbPool::Sqlite(
                SqlitePoolOptions::new()
                    .max_connections(5)
                    .connect_with(options)
                    .await?,
            )
        };
        let db = Self { pool };
        db.migrate().await?;
        db.backfill_rollups_from_raw().await?;
        db.prune_expired(now_ms()).await?;
        Ok(db)
    }

    async fn migrate(&self) -> anyhow::Result<()> {
        match &self.pool {
            DbPool::Postgres(pool) => self.migrate_postgres(pool).await,
            DbPool::Sqlite(pool) => self.migrate_sqlite(pool).await,
        }
    }

    async fn migrate_sqlite(&self, pool: &SqlitePool) -> anyhow::Result<()> {
        sqlx::query("PRAGMA journal_mode = WAL")
            .execute(pool)
            .await?;
        sqlx::query("PRAGMA synchronous = NORMAL")
            .execute(pool)
            .await?;
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS collectors (
                collector_id TEXT PRIMARY KEY,
                display_name TEXT NOT NULL UNIQUE,
                device_model TEXT NOT NULL,
                client_platform TEXT NOT NULL,
                app_version TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                last_bpm INTEGER,
                last_seen_ms INTEGER,
                updated_at_ms INTEGER,
                token TEXT NOT NULL UNIQUE
            )
            "#,
        )
        .execute(pool)
        .await?;
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS collector_seqs (
                collector_id TEXT NOT NULL,
                seq INTEGER NOT NULL,
                seen_at_ms INTEGER NOT NULL,
                PRIMARY KEY (collector_id, seq),
                FOREIGN KEY (collector_id) REFERENCES collectors(collector_id) ON DELETE CASCADE
            )
            "#,
        )
        .execute(pool)
        .await?;
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS heart_rate_samples (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                collector_id TEXT NOT NULL,
                t_ms INTEGER NOT NULL,
                bpm INTEGER NOT NULL,
                seq INTEGER NOT NULL,
                received_at_ms INTEGER NOT NULL,
                FOREIGN KEY (collector_id) REFERENCES collectors(collector_id) ON DELETE CASCADE
            )
            "#,
        )
        .execute(pool)
        .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_samples_collector_time ON heart_rate_samples(collector_id, t_ms)",
        )
        .execute(pool)
        .await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_samples_time ON heart_rate_samples(t_ms)")
            .execute(pool)
            .await?;
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS heart_rate_rollups (
                collector_id TEXT NOT NULL,
                bucket_ms INTEGER NOT NULL,
                sample_count INTEGER NOT NULL,
                bpm_sum INTEGER NOT NULL,
                bpm_sum_sq INTEGER NOT NULL DEFAULT 0,
                min_bpm INTEGER NOT NULL,
                max_bpm INTEGER NOT NULL,
                first_t_ms INTEGER NOT NULL,
                last_t_ms INTEGER NOT NULL,
                PRIMARY KEY (collector_id, bucket_ms),
                FOREIGN KEY (collector_id) REFERENCES collectors(collector_id) ON DELETE CASCADE
            )
            "#,
        )
        .execute(pool)
        .await?;
        ignore_duplicate_column(
            sqlx::query(
                "ALTER TABLE heart_rate_rollups ADD COLUMN bpm_sum_sq INTEGER NOT NULL DEFAULT 0",
            )
            .execute(pool)
            .await,
        )?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_rollups_collector_bucket ON heart_rate_rollups(collector_id, bucket_ms)",
        )
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn migrate_postgres(&self, pool: &PgPool) -> anyhow::Result<()> {
        if let Err(error) = sqlx::query("CREATE EXTENSION IF NOT EXISTS timescaledb")
            .execute(pool)
            .await
        {
            tracing::warn!(
                ?error,
                "TimescaleDB extension is unavailable; using plain PostgreSQL indexes"
            );
        }
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS collectors (
                collector_id TEXT PRIMARY KEY,
                display_name TEXT NOT NULL UNIQUE,
                device_model TEXT NOT NULL,
                client_platform TEXT NOT NULL,
                app_version TEXT NOT NULL,
                created_at_ms BIGINT NOT NULL,
                last_bpm INTEGER,
                last_seen_ms BIGINT,
                updated_at_ms BIGINT,
                token TEXT NOT NULL UNIQUE
            )
            "#,
        )
        .execute(pool)
        .await?;
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS collector_seqs (
                collector_id TEXT NOT NULL REFERENCES collectors(collector_id) ON DELETE CASCADE,
                seq BIGINT NOT NULL,
                seen_at_ms BIGINT NOT NULL,
                PRIMARY KEY (collector_id, seq)
            )
            "#,
        )
        .execute(pool)
        .await?;
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS heart_rate_samples (
                id BIGSERIAL,
                collector_id TEXT NOT NULL REFERENCES collectors(collector_id) ON DELETE CASCADE,
                t_ms BIGINT NOT NULL,
                bpm INTEGER NOT NULL,
                seq BIGINT NOT NULL,
                received_at_ms BIGINT NOT NULL,
                PRIMARY KEY (collector_id, t_ms, id)
            )
            "#,
        )
        .execute(pool)
        .await?;
        if let Err(error) = sqlx::query(
            r#"
            SELECT create_hypertable(
                'heart_rate_samples',
                't_ms',
                if_not_exists => TRUE,
                chunk_time_interval => 3600000
            )
            "#,
        )
        .execute(pool)
        .await
        {
            tracing::warn!(
                ?error,
                "heart_rate_samples was not converted to a TimescaleDB hypertable"
            );
        }
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_samples_collector_time ON heart_rate_samples(collector_id, t_ms DESC)",
        )
        .execute(pool)
        .await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_samples_time ON heart_rate_samples(t_ms DESC)")
            .execute(pool)
            .await?;
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS heart_rate_rollups (
                collector_id TEXT NOT NULL REFERENCES collectors(collector_id) ON DELETE CASCADE,
                bucket_ms BIGINT NOT NULL,
                sample_count BIGINT NOT NULL,
                bpm_sum BIGINT NOT NULL,
                bpm_sum_sq BIGINT NOT NULL DEFAULT 0,
                min_bpm INTEGER NOT NULL,
                max_bpm INTEGER NOT NULL,
                first_t_ms BIGINT NOT NULL,
                last_t_ms BIGINT NOT NULL,
                PRIMARY KEY (collector_id, bucket_ms)
            )
            "#,
        )
        .execute(pool)
        .await?;
        sqlx::query(
            "ALTER TABLE heart_rate_rollups ADD COLUMN IF NOT EXISTS bpm_sum_sq BIGINT NOT NULL DEFAULT 0",
        )
        .execute(pool)
        .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_rollups_collector_bucket ON heart_rate_rollups(collector_id, bucket_ms)",
        )
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn backfill_rollups_from_raw(&self) -> anyhow::Result<()> {
        match &self.pool {
            DbPool::Sqlite(pool) => self.backfill_rollups_from_raw_sqlite(pool).await,
            DbPool::Postgres(pool) => self.backfill_rollups_from_raw_postgres(pool).await,
        }
    }

    async fn backfill_rollups_from_raw_sqlite(&self, pool: &SqlitePool) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            INSERT INTO heart_rate_rollups (
                collector_id, bucket_ms, sample_count, bpm_sum, bpm_sum_sq,
                min_bpm, max_bpm, first_t_ms, last_t_ms
            )
            SELECT
                collector_id,
                (t_ms / ?) * ? AS bucket_ms,
                COUNT(*) AS sample_count,
                SUM(bpm) AS bpm_sum,
                SUM(CAST(bpm AS INTEGER) * CAST(bpm AS INTEGER)) AS bpm_sum_sq,
                MIN(bpm) AS min_bpm,
                MAX(bpm) AS max_bpm,
                MIN(t_ms) AS first_t_ms,
                MAX(t_ms) AS last_t_ms
            FROM heart_rate_samples
            WHERE true
            GROUP BY 1, 2
            ON CONFLICT(collector_id, bucket_ms) DO UPDATE SET
                sample_count = excluded.sample_count,
                bpm_sum = excluded.bpm_sum,
                bpm_sum_sq = excluded.bpm_sum_sq,
                min_bpm = excluded.min_bpm,
                max_bpm = excluded.max_bpm,
                first_t_ms = excluded.first_t_ms,
                last_t_ms = excluded.last_t_ms
            "#,
        )
        .bind(ROLLUP_BUCKET_MS)
        .bind(ROLLUP_BUCKET_MS)
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn backfill_rollups_from_raw_postgres(&self, pool: &PgPool) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            INSERT INTO heart_rate_rollups (
                collector_id, bucket_ms, sample_count, bpm_sum, bpm_sum_sq,
                min_bpm, max_bpm, first_t_ms, last_t_ms
            )
            SELECT
                collector_id,
                (t_ms / $1) * $1 AS bucket_ms,
                COUNT(*) AS sample_count,
                SUM(bpm)::BIGINT AS bpm_sum,
                SUM((bpm::BIGINT * bpm::BIGINT))::BIGINT AS bpm_sum_sq,
                MIN(bpm) AS min_bpm,
                MAX(bpm) AS max_bpm,
                MIN(t_ms) AS first_t_ms,
                MAX(t_ms) AS last_t_ms
            FROM heart_rate_samples
            GROUP BY 1, 2
            ON CONFLICT(collector_id, bucket_ms) DO UPDATE SET
                sample_count = excluded.sample_count,
                bpm_sum = excluded.bpm_sum,
                bpm_sum_sq = excluded.bpm_sum_sq,
                min_bpm = excluded.min_bpm,
                max_bpm = excluded.max_bpm,
                first_t_ms = excluded.first_t_ms,
                last_t_ms = excluded.last_t_ms
            "#,
        )
        .bind(ROLLUP_BUCKET_MS)
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn prune_expired(&self, current_ms: i64) -> anyhow::Result<()> {
        match &self.pool {
            DbPool::Sqlite(pool) => {
                sqlx::query("DELETE FROM heart_rate_samples WHERE t_ms < ?")
                    .bind(current_ms - RAW_SAMPLE_TTL_MS)
                    .execute(pool)
                    .await?;
                sqlx::query("DELETE FROM heart_rate_rollups WHERE bucket_ms < ?")
                    .bind(current_ms - ROLLUP_TTL_MS)
                    .execute(pool)
                    .await?;
            }
            DbPool::Postgres(pool) => {
                sqlx::query("DELETE FROM heart_rate_samples WHERE t_ms < $1")
                    .bind(current_ms - RAW_SAMPLE_TTL_MS)
                    .execute(pool)
                    .await?;
                sqlx::query("DELETE FROM heart_rate_rollups WHERE bucket_ms < $1")
                    .bind(current_ms - ROLLUP_TTL_MS)
                    .execute(pool)
                    .await?;
            }
        }
        Ok(())
    }

    async fn load_store(&self, sample_cutoff_ms: i64, current_ms: i64) -> anyhow::Result<Store> {
        match &self.pool {
            DbPool::Postgres(pool) => {
                self.load_store_postgres(pool, sample_cutoff_ms, current_ms)
                    .await
            }
            DbPool::Sqlite(pool) => {
                self.load_store_sqlite(pool, sample_cutoff_ms, current_ms)
                    .await
            }
        }
    }

    async fn load_store_sqlite(
        &self,
        pool: &SqlitePool,
        sample_cutoff_ms: i64,
        current_ms: i64,
    ) -> anyhow::Result<Store> {
        let mut store = Store::default();
        let collectors = sqlx::query(
            r#"
            SELECT collector_id, display_name, device_model, client_platform, app_version,
                   created_at_ms, last_bpm, last_seen_ms, updated_at_ms, token
            FROM collectors
            "#,
        )
        .fetch_all(pool)
        .await?;
        for row in collectors {
            let collector_id: String = row.get("collector_id");
            let token: String = row.get("token");
            store.tokens.insert(token.clone(), collector_id.clone());
            store
                .collector_tokens
                .insert(collector_id.clone(), token.clone());
            store.collectors.insert(
                collector_id.clone(),
                Collector {
                    collector_id,
                    display_name: row.get("display_name"),
                    device_model: row.get("device_model"),
                    _client_platform: row.get("client_platform"),
                    _app_version: row.get("app_version"),
                    _created_at_ms: row.get("created_at_ms"),
                    last_bpm: row.get("last_bpm"),
                    last_seen_ms: row.get("last_seen_ms"),
                    updated_at_ms: row.get("updated_at_ms"),
                    seen_seqs: HashSet::new(),
                    seq_order: VecDeque::new(),
                    series: VecDeque::new(),
                },
            );
        }

        let seq_rows = sqlx::query(
            r#"
            SELECT collector_id, seq
            FROM collector_seqs
            ORDER BY collector_id, seen_at_ms DESC
            "#,
        )
        .fetch_all(pool)
        .await?;
        let mut loaded_seq_counts: HashMap<String, usize> = HashMap::new();
        for row in seq_rows {
            let collector_id: String = row.get("collector_id");
            let count = loaded_seq_counts.entry(collector_id.clone()).or_default();
            if *count >= 512 {
                continue;
            }
            if let Some(collector) = store.collectors.get_mut(&collector_id) {
                let seq: u64 = row.get::<i64, _>("seq") as u64;
                collector.seen_seqs.insert(seq);
                collector.seq_order.push_back(seq);
                *count += 1;
            }
        }

        let sample_rows = sqlx::query(
            r#"
            SELECT collector_id, t_ms, bpm
            FROM heart_rate_samples
            WHERE t_ms >= ?
            ORDER BY collector_id, t_ms
            "#,
        )
        .bind(sample_cutoff_ms)
        .fetch_all(pool)
        .await?;
        for row in sample_rows {
            let collector_id: String = row.get("collector_id");
            if let Some(collector) = store.collectors.get_mut(&collector_id) {
                collector.series.push_back(SeriesSample {
                    t_ms: row.get("t_ms"),
                    bpm: row.get("bpm"),
                });
            }
        }
        reconcile_loaded_latest(&mut store, current_ms);
        Ok(store)
    }

    async fn load_store_postgres(
        &self,
        pool: &PgPool,
        sample_cutoff_ms: i64,
        current_ms: i64,
    ) -> anyhow::Result<Store> {
        let mut store = Store::default();
        let collectors = sqlx::query(
            r#"
            SELECT collector_id, display_name, device_model, client_platform, app_version,
                   created_at_ms, last_bpm, last_seen_ms, updated_at_ms, token
            FROM collectors
            "#,
        )
        .fetch_all(pool)
        .await?;
        for row in collectors {
            let collector_id: String = row.get("collector_id");
            let token: String = row.get("token");
            store.tokens.insert(token.clone(), collector_id.clone());
            store
                .collector_tokens
                .insert(collector_id.clone(), token.clone());
            store.collectors.insert(
                collector_id.clone(),
                Collector {
                    collector_id,
                    display_name: row.get("display_name"),
                    device_model: row.get("device_model"),
                    _client_platform: row.get("client_platform"),
                    _app_version: row.get("app_version"),
                    _created_at_ms: row.get("created_at_ms"),
                    last_bpm: row.get("last_bpm"),
                    last_seen_ms: row.get("last_seen_ms"),
                    updated_at_ms: row.get("updated_at_ms"),
                    seen_seqs: HashSet::new(),
                    seq_order: VecDeque::new(),
                    series: VecDeque::new(),
                },
            );
        }

        let seq_rows = sqlx::query(
            r#"
            SELECT collector_id, seq
            FROM collector_seqs
            ORDER BY collector_id, seen_at_ms DESC
            "#,
        )
        .fetch_all(pool)
        .await?;
        let mut loaded_seq_counts: HashMap<String, usize> = HashMap::new();
        for row in seq_rows {
            let collector_id: String = row.get("collector_id");
            let count = loaded_seq_counts.entry(collector_id.clone()).or_default();
            if *count >= 512 {
                continue;
            }
            if let Some(collector) = store.collectors.get_mut(&collector_id) {
                let seq: u64 = row.get::<i64, _>("seq") as u64;
                collector.seen_seqs.insert(seq);
                collector.seq_order.push_back(seq);
                *count += 1;
            }
        }

        let sample_rows = sqlx::query(
            r#"
            SELECT collector_id, t_ms, bpm
            FROM heart_rate_samples
            WHERE t_ms >= $1
            ORDER BY collector_id, t_ms
            "#,
        )
        .bind(sample_cutoff_ms)
        .fetch_all(pool)
        .await?;
        for row in sample_rows {
            let collector_id: String = row.get("collector_id");
            if let Some(collector) = store.collectors.get_mut(&collector_id) {
                collector.series.push_back(SeriesSample {
                    t_ms: row.get("t_ms"),
                    bpm: row.get("bpm"),
                });
            }
        }
        reconcile_loaded_latest(&mut store, current_ms);
        Ok(store)
    }

    async fn insert_collector(&self, collector: &Collector, token: &str) -> anyhow::Result<()> {
        match &self.pool {
            DbPool::Sqlite(pool) => {
                sqlx::query(
                    r#"
            INSERT INTO collectors (
                collector_id, display_name, device_model, client_platform, app_version,
                created_at_ms, last_bpm, last_seen_ms, updated_at_ms, token
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(display_name) DO UPDATE SET
                device_model = excluded.device_model,
                client_platform = excluded.client_platform,
                app_version = excluded.app_version,
                token = excluded.token
            "#,
                )
                .bind(&collector.collector_id)
                .bind(&collector.display_name)
                .bind(&collector.device_model)
                .bind(&collector._client_platform)
                .bind(&collector._app_version)
                .bind(collector._created_at_ms)
                .bind(collector.last_bpm)
                .bind(collector.last_seen_ms)
                .bind(collector.updated_at_ms)
                .bind(token)
                .execute(pool)
                .await?;
            }
            DbPool::Postgres(pool) => {
                sqlx::query(
                    r#"
            INSERT INTO collectors (
                collector_id, display_name, device_model, client_platform, app_version,
                created_at_ms, last_bpm, last_seen_ms, updated_at_ms, token
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT(display_name) DO UPDATE SET
                device_model = excluded.device_model,
                client_platform = excluded.client_platform,
                app_version = excluded.app_version,
                token = excluded.token
            "#,
                )
                .bind(&collector.collector_id)
                .bind(&collector.display_name)
                .bind(&collector.device_model)
                .bind(&collector._client_platform)
                .bind(&collector._app_version)
                .bind(collector._created_at_ms)
                .bind(collector.last_bpm)
                .bind(collector.last_seen_ms)
                .bind(collector.updated_at_ms)
                .bind(token)
                .execute(pool)
                .await?;
            }
        }
        Ok(())
    }

    async fn update_collector_session(
        &self,
        collector_id: &str,
        device_model: &str,
        client_platform: &str,
        app_version: &str,
        token: &str,
    ) -> anyhow::Result<()> {
        match &self.pool {
            DbPool::Sqlite(pool) => {
                sqlx::query(
                    r#"
            UPDATE collectors
            SET device_model = ?, client_platform = ?, app_version = ?, token = ?
            WHERE collector_id = ?
            "#,
                )
                .bind(device_model)
                .bind(client_platform)
                .bind(app_version)
                .bind(token)
                .bind(collector_id)
                .execute(pool)
                .await?;
                sqlx::query("DELETE FROM collector_seqs WHERE collector_id = ?")
                    .bind(collector_id)
                    .execute(pool)
                    .await?;
            }
            DbPool::Postgres(pool) => {
                sqlx::query(
                    r#"
            UPDATE collectors
            SET device_model = $1, client_platform = $2, app_version = $3, token = $4
            WHERE collector_id = $5
            "#,
                )
                .bind(device_model)
                .bind(client_platform)
                .bind(app_version)
                .bind(token)
                .bind(collector_id)
                .execute(pool)
                .await?;
                sqlx::query("DELETE FROM collector_seqs WHERE collector_id = $1")
                    .bind(collector_id)
                    .execute(pool)
                    .await?;
            }
        }
        Ok(())
    }

    async fn persist_ingest(
        &self,
        collector: &Collector,
        seq: u64,
        samples: &[SeriesSample],
        recv_ms: i64,
    ) -> anyhow::Result<()> {
        match &self.pool {
            DbPool::Sqlite(pool) => {
                self.persist_ingest_sqlite(pool, collector, seq, samples, recv_ms)
                    .await
            }
            DbPool::Postgres(pool) => {
                self.persist_ingest_postgres(pool, collector, seq, samples, recv_ms)
                    .await
            }
        }
    }

    async fn series(
        &self,
        collector_id: &str,
        cutoff_ms: i64,
        upper_ms: i64,
        window_ms: i64,
        max_points: i64,
    ) -> anyhow::Result<Vec<SeriesSample>> {
        let bucket_ms = (window_ms / max_points.max(1)).max(1);
        let mut samples = match &self.pool {
            DbPool::Sqlite(pool) => {
                self.series_sqlite(pool, collector_id, cutoff_ms, upper_ms, bucket_ms)
                    .await
            }
            DbPool::Postgres(pool) => {
                self.series_postgres(pool, collector_id, cutoff_ms, upper_ms, bucket_ms)
                    .await
            }
        }?;
        if let Some(latest) = self
            .latest_raw_sample(collector_id, cutoff_ms, upper_ms)
            .await?
        {
            merge_latest_sample(&mut samples, latest);
        }
        Ok(samples)
    }

    async fn series_sqlite(
        &self,
        pool: &SqlitePool,
        collector_id: &str,
        cutoff_ms: i64,
        upper_ms: i64,
        bucket_ms: i64,
    ) -> anyhow::Result<Vec<SeriesSample>> {
        let rows = sqlx::query(
            r#"
            SELECT CAST(AVG(t_ms) AS INTEGER) AS t_ms, CAST(ROUND(AVG(bpm)) AS INTEGER) AS bpm
            FROM heart_rate_samples
            WHERE collector_id = ? AND t_ms >= ? AND t_ms <= ?
            GROUP BY ((t_ms - ?) / ?)
            ORDER BY t_ms
            "#,
        )
        .bind(collector_id)
        .bind(cutoff_ms)
        .bind(upper_ms)
        .bind(cutoff_ms)
        .bind(bucket_ms)
        .fetch_all(pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| SeriesSample {
                t_ms: row.get("t_ms"),
                bpm: row.get("bpm"),
            })
            .collect())
    }

    async fn series_postgres(
        &self,
        pool: &PgPool,
        collector_id: &str,
        cutoff_ms: i64,
        upper_ms: i64,
        bucket_ms: i64,
    ) -> anyhow::Result<Vec<SeriesSample>> {
        let rows = sqlx::query(
            r#"
            SELECT AVG(t_ms)::BIGINT AS t_ms, ROUND(AVG(bpm))::INTEGER AS bpm
            FROM heart_rate_samples
            WHERE collector_id = $1 AND t_ms >= $2 AND t_ms <= $3
            GROUP BY ((t_ms - $4) / $5)
            ORDER BY t_ms
            "#,
        )
        .bind(collector_id)
        .bind(cutoff_ms)
        .bind(upper_ms)
        .bind(cutoff_ms)
        .bind(bucket_ms)
        .fetch_all(pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| SeriesSample {
                t_ms: row.get("t_ms"),
                bpm: row.get("bpm"),
            })
            .collect())
    }

    async fn latest_raw_sample(
        &self,
        collector_id: &str,
        cutoff_ms: i64,
        upper_ms: i64,
    ) -> anyhow::Result<Option<SeriesSample>> {
        match &self.pool {
            DbPool::Sqlite(pool) => {
                self.latest_raw_sample_sqlite(pool, collector_id, cutoff_ms, upper_ms)
                    .await
            }
            DbPool::Postgres(pool) => {
                self.latest_raw_sample_postgres(pool, collector_id, cutoff_ms, upper_ms)
                    .await
            }
        }
    }

    async fn latest_raw_sample_sqlite(
        &self,
        pool: &SqlitePool,
        collector_id: &str,
        cutoff_ms: i64,
        upper_ms: i64,
    ) -> anyhow::Result<Option<SeriesSample>> {
        let row = sqlx::query(
            r#"
            SELECT t_ms, bpm
            FROM heart_rate_samples
            WHERE collector_id = ? AND t_ms >= ? AND t_ms <= ?
            ORDER BY t_ms DESC, received_at_ms DESC
            LIMIT 1
            "#,
        )
        .bind(collector_id)
        .bind(cutoff_ms)
        .bind(upper_ms)
        .fetch_optional(pool)
        .await?;
        Ok(row.map(|row| SeriesSample {
            t_ms: row.get("t_ms"),
            bpm: row.get("bpm"),
        }))
    }

    async fn latest_raw_sample_postgres(
        &self,
        pool: &PgPool,
        collector_id: &str,
        cutoff_ms: i64,
        upper_ms: i64,
    ) -> anyhow::Result<Option<SeriesSample>> {
        let row = sqlx::query(
            r#"
            SELECT t_ms, bpm
            FROM heart_rate_samples
            WHERE collector_id = $1 AND t_ms >= $2 AND t_ms <= $3
            ORDER BY t_ms DESC, received_at_ms DESC
            LIMIT 1
            "#,
        )
        .bind(collector_id)
        .bind(cutoff_ms)
        .bind(upper_ms)
        .fetch_optional(pool)
        .await?;
        Ok(row.map(|row| SeriesSample {
            t_ms: row.get("t_ms"),
            bpm: row.get("bpm"),
        }))
    }

    async fn persist_ingest_sqlite(
        &self,
        pool: &SqlitePool,
        collector: &Collector,
        seq: u64,
        samples: &[SeriesSample],
        recv_ms: i64,
    ) -> anyhow::Result<()> {
        let mut tx = pool.begin().await?;
        sqlx::query(
            r#"
            UPDATE collectors
            SET display_name = ?, device_model = ?, last_bpm = ?, last_seen_ms = ?, updated_at_ms = ?
            WHERE collector_id = ?
            "#,
        )
        .bind(&collector.display_name)
        .bind(&collector.device_model)
        .bind(collector.last_bpm)
        .bind(collector.last_seen_ms)
        .bind(collector.updated_at_ms)
        .bind(&collector.collector_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT OR IGNORE INTO collector_seqs (collector_id, seq, seen_at_ms) VALUES (?, ?, ?)",
        )
        .bind(&collector.collector_id)
        .bind(seq as i64)
        .bind(recv_ms)
        .execute(&mut *tx)
        .await?;
        for sample in samples {
            sqlx::query(
                r#"
                INSERT INTO heart_rate_samples (collector_id, t_ms, bpm, seq, received_at_ms)
                VALUES (?, ?, ?, ?, ?)
                "#,
            )
            .bind(&collector.collector_id)
            .bind(sample.t_ms)
            .bind(sample.bpm)
            .bind(seq as i64)
            .bind(recv_ms)
            .execute(&mut *tx)
            .await?;
            let bucket_ms = rollup_bucket(sample.t_ms);
            sqlx::query(
                r#"
                INSERT INTO heart_rate_rollups (
                    collector_id, bucket_ms, sample_count, bpm_sum, bpm_sum_sq,
                    min_bpm, max_bpm, first_t_ms, last_t_ms
                )
                VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(collector_id, bucket_ms) DO UPDATE SET
                    sample_count = sample_count + 1,
                    bpm_sum = bpm_sum + excluded.bpm_sum,
                    bpm_sum_sq = bpm_sum_sq + excluded.bpm_sum_sq,
                    min_bpm = MIN(min_bpm, excluded.min_bpm),
                    max_bpm = MAX(max_bpm, excluded.max_bpm),
                    first_t_ms = MIN(first_t_ms, excluded.first_t_ms),
                    last_t_ms = MAX(last_t_ms, excluded.last_t_ms)
                "#,
            )
            .bind(&collector.collector_id)
            .bind(bucket_ms)
            .bind(sample.bpm as i64)
            .bind(sample.bpm as i64 * sample.bpm as i64)
            .bind(sample.bpm)
            .bind(sample.bpm)
            .bind(sample.t_ms)
            .bind(sample.t_ms)
            .execute(&mut *tx)
            .await?;
        }
        sqlx::query("DELETE FROM heart_rate_samples WHERE collector_id = ? AND t_ms < ?")
            .bind(&collector.collector_id)
            .bind(recv_ms - RAW_SAMPLE_TTL_MS)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM heart_rate_rollups WHERE collector_id = ? AND bucket_ms < ?")
            .bind(&collector.collector_id)
            .bind(recv_ms - ROLLUP_TTL_MS)
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            r#"
            DELETE FROM collector_seqs
            WHERE collector_id = ?
              AND seq NOT IN (
                  SELECT seq FROM collector_seqs
                  WHERE collector_id = ?
                  ORDER BY seen_at_ms DESC
                  LIMIT 512
              )
            "#,
        )
        .bind(&collector.collector_id)
        .bind(&collector.collector_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn persist_ingest_postgres(
        &self,
        pool: &PgPool,
        collector: &Collector,
        seq: u64,
        samples: &[SeriesSample],
        recv_ms: i64,
    ) -> anyhow::Result<()> {
        let mut tx = pool.begin().await?;
        sqlx::query(
            r#"
            UPDATE collectors
            SET display_name = $1, device_model = $2, last_bpm = $3, last_seen_ms = $4, updated_at_ms = $5
            WHERE collector_id = $6
            "#,
        )
        .bind(&collector.display_name)
        .bind(&collector.device_model)
        .bind(collector.last_bpm)
        .bind(collector.last_seen_ms)
        .bind(collector.updated_at_ms)
        .bind(&collector.collector_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO collector_seqs (collector_id, seq, seen_at_ms) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
        )
        .bind(&collector.collector_id)
        .bind(seq as i64)
        .bind(recv_ms)
        .execute(&mut *tx)
        .await?;
        for sample in samples {
            sqlx::query(
                r#"
                INSERT INTO heart_rate_samples (collector_id, t_ms, bpm, seq, received_at_ms)
                VALUES ($1, $2, $3, $4, $5)
                "#,
            )
            .bind(&collector.collector_id)
            .bind(sample.t_ms)
            .bind(sample.bpm)
            .bind(seq as i64)
            .bind(recv_ms)
            .execute(&mut *tx)
            .await?;
            let bucket_ms = rollup_bucket(sample.t_ms);
            sqlx::query(
                r#"
                INSERT INTO heart_rate_rollups (
                    collector_id, bucket_ms, sample_count, bpm_sum, bpm_sum_sq,
                    min_bpm, max_bpm, first_t_ms, last_t_ms
                )
                VALUES ($1, $2, 1, $3, $4, $5, $6, $7, $8)
                ON CONFLICT(collector_id, bucket_ms) DO UPDATE SET
                    sample_count = heart_rate_rollups.sample_count + 1,
                    bpm_sum = heart_rate_rollups.bpm_sum + excluded.bpm_sum,
                    bpm_sum_sq = heart_rate_rollups.bpm_sum_sq + excluded.bpm_sum_sq,
                    min_bpm = LEAST(heart_rate_rollups.min_bpm, excluded.min_bpm),
                    max_bpm = GREATEST(heart_rate_rollups.max_bpm, excluded.max_bpm),
                    first_t_ms = LEAST(heart_rate_rollups.first_t_ms, excluded.first_t_ms),
                    last_t_ms = GREATEST(heart_rate_rollups.last_t_ms, excluded.last_t_ms)
                "#,
            )
            .bind(&collector.collector_id)
            .bind(bucket_ms)
            .bind(sample.bpm as i64)
            .bind(sample.bpm as i64 * sample.bpm as i64)
            .bind(sample.bpm)
            .bind(sample.bpm)
            .bind(sample.t_ms)
            .bind(sample.t_ms)
            .execute(&mut *tx)
            .await?;
        }
        sqlx::query("DELETE FROM heart_rate_samples WHERE collector_id = $1 AND t_ms < $2")
            .bind(&collector.collector_id)
            .bind(recv_ms - RAW_SAMPLE_TTL_MS)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM heart_rate_rollups WHERE collector_id = $1 AND bucket_ms < $2")
            .bind(&collector.collector_id)
            .bind(recv_ms - ROLLUP_TTL_MS)
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            r#"
            DELETE FROM collector_seqs
            WHERE collector_id = $1
              AND seq NOT IN (
                  SELECT seq FROM collector_seqs
                  WHERE collector_id = $2
                  ORDER BY seen_at_ms DESC
                  LIMIT 512
              )
            "#,
        )
        .bind(&collector.collector_id)
        .bind(&collector.collector_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let app = app().await?;
    let addr: SocketAddr = std::env::var("HEARTWITH_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8000".to_string())
        .parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("heartwith server listening on http://{addr}");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn app() -> anyhow::Result<Router> {
    let database_url = std::env::var("HEARTWITH_DATABASE_URL")
        .unwrap_or_else(|_| "sqlite://heartwith.db".to_string());
    app_with_database(&database_url).await
}

async fn app_with_database(database_url: &str) -> anyhow::Result<Router> {
    let db = Db::connect(database_url).await?;
    let current_ms = now_ms();
    let store = db
        .load_store(current_ms - MEMORY_SERIES_TTL_MS, current_ms)
        .await?;
    let (events, _) = broadcast::channel(256);
    start_retention_sweeper(db.clone());
    let state = AppState {
        inner: Arc::new(RwLock::new(store)),
        events,
        db,
    };

    let web_dir = web_dir();

    let static_service = ServiceBuilder::new()
        .layer(SetResponseHeaderLayer::overriding(
            header::CACHE_CONTROL,
            HeaderValue::from_static("no-cache, max-age=0, must-revalidate"),
        ))
        .service(
            ServeDir::new(web_dir)
                .precompressed_gzip()
                .append_index_html_on_directories(true),
        );

    let router = Router::new()
        .route("/api/v1/collector/sessions", post(create_session))
        .route("/api/v1/hr/batches", post(ingest_batch))
        .route("/api/v1/lobby/participants", get(lobby_participants))
        .route("/api/v1/lobby/events", get(lobby_events))
        .route(
            "/api/v1/participants/{collector_id}/series",
            get(participant_series),
        )
        .fallback_service(static_service)
        .layer(CorsLayer::permissive())
        .with_state(state);
    Ok(router)
}

fn web_dir() -> PathBuf {
    if let Ok(path) = std::env::var("HEARTWITH_WEB_DIR") {
        let path = PathBuf::from(path);
        if path.exists() {
            return path;
        }
    }

    let production = PathBuf::from("clients/heartwith-web/build/dist/wasmJs/productionExecutable");
    if production.exists() {
        return production;
    }

    let development =
        PathBuf::from("clients/heartwith-web/build/kotlin-webpack/wasmJs/developmentExecutable");
    if development.exists() {
        return development;
    }

    let web = PathBuf::from("web");
    if web.exists() {
        return web;
    }

    PathBuf::from("web-fallback")
}

async fn create_session(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<SessionRequest>,
) -> Result<Json<SessionResponse>, ApiError> {
    let display_name = truncate(&request.display_name, 80);
    let device_model = truncate(&request.device_model, 80);
    let client_platform = truncate(&request.client_platform, 32);
    let app_version = truncate(&request.app_version, 32);
    let collector_id = format!("col_{}", Uuid::new_v4().simple());
    let token =
        Uuid::new_v4().as_hyphenated().to_string() + "." + &Uuid::new_v4().simple().to_string();
    let base_url = public_base_url(&headers);
    let mut store = state.inner.write().await;

    if let Some(existing_id) = store
        .collectors
        .values()
        .find(|collector| collector.display_name == display_name)
        .map(|collector| collector.collector_id.clone())
    {
        let Some(updated_collector) = store.collectors.get_mut(&existing_id).map(|collector| {
            collector.device_model = device_model;
            collector._client_platform = client_platform;
            collector._app_version = app_version;
            collector.seen_seqs.clear();
            collector.seq_order.clear();
            collector.clone()
        }) else {
            return Err(ApiError::Unauthorized);
        };
        let token = store
            .collector_tokens
            .get(&existing_id)
            .cloned()
            .unwrap_or_else(|| {
                let token = Uuid::new_v4().as_hyphenated().to_string()
                    + "."
                    + &Uuid::new_v4().simple().to_string();
                store.tokens.insert(token.clone(), existing_id.clone());
                store
                    .collector_tokens
                    .insert(existing_id.clone(), token.clone());
                token
            });
        state
            .db
            .update_collector_session(
                &existing_id,
                &updated_collector.device_model,
                &updated_collector._client_platform,
                &updated_collector._app_version,
                &token,
            )
            .await?;
        return Ok(Json(SessionResponse {
            collector_id: existing_id,
            collector_token: token,
            ingest_url: format!("{base_url}/api/v1/hr/batches"),
            policy: UploadPolicy::default(),
        }));
    }

    let collector = Collector {
        collector_id: collector_id.clone(),
        display_name,
        device_model,
        _client_platform: client_platform,
        _app_version: app_version,
        _created_at_ms: now_ms(),
        last_bpm: None,
        last_seen_ms: None,
        updated_at_ms: None,
        seen_seqs: HashSet::new(),
        seq_order: VecDeque::new(),
        series: VecDeque::new(),
    };

    store.tokens.insert(token.clone(), collector_id.clone());
    store
        .collector_tokens
        .insert(collector_id.clone(), token.clone());
    store.collectors.insert(collector_id.clone(), collector);
    let collector = store
        .collectors
        .get(&collector_id)
        .expect("collector inserted")
        .clone();
    state.db.insert_collector(&collector, &token).await?;

    Ok(Json(SessionResponse {
        collector_id,
        collector_token: token,
        ingest_url: format!("{base_url}/api/v1/hr/batches"),
        policy: UploadPolicy::default(),
    }))
}

async fn ingest_batch(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<IngestResponse>, ApiError> {
    let token = bearer_token(&headers).ok_or(ApiError::Unauthorized)?;
    let bytes = to_bytes(body, MAX_BODY_BYTES)
        .await
        .map_err(|_| ApiError::BadRequest("invalid body"))?;
    let payload = decode_payload(&headers, &bytes)?;
    if payload.schema != 1 {
        return Err(ApiError::BadRequest("unsupported schema"));
    }

    let payload_seq = payload.seq;
    let mut event_display_name = None;
    let mut persist_collector = None;
    let mut persist_samples = Vec::new();
    let mut persist_recv_ms = 0;
    let accepted = {
        let mut store = state.inner.write().await;
        let Some(collector_id) = store.tokens.get(token).cloned() else {
            return Err(ApiError::Unauthorized);
        };
        if collector_id != payload.collector_id {
            return Err(ApiError::Unauthorized);
        }
        let Some(collector) = store.collectors.get_mut(&collector_id) else {
            return Err(ApiError::Unauthorized);
        };
        if collector.seen_seqs.contains(&payload_seq) {
            return Ok(Json(IngestResponse {
                ok: true,
                accepted: 0,
                server_time_ms: now_ms(),
                next_policy: None,
            }));
        }

        let recv_ms = now_ms();
        let anchor_ms = if (payload.sent_at_ms - recv_ms).abs() > MAX_CLOCK_SKEW_MS {
            recv_ms
        } else {
            payload.sent_at_ms
        };
        let mut accepted = 0;
        let mut latest_changed = false;
        let mut rejected_future_samples = 0usize;
        let mut max_future_offset_ms = 0i64;

        for sample in payload.samples {
            if !(MIN_BPM..=MAX_BPM).contains(&sample.bpm) {
                continue;
            }
            let raw_t_ms = anchor_ms + sample.dt_ms;
            if raw_t_ms > recv_ms + MAX_SAMPLE_FUTURE_MS {
                rejected_future_samples += 1;
                max_future_offset_ms = max_future_offset_ms.max(raw_t_ms - recv_ms);
                continue;
            }
            let t_ms = raw_t_ms;
            let series_sample = SeriesSample {
                t_ms,
                bpm: sample.bpm,
            };
            collector.series.push_back(series_sample.clone());
            persist_samples.push(series_sample);
            accepted += 1;
            let comparable_last_seen_ms = collector.last_seen_ms.map(|last| last.min(recv_ms));
            if comparable_last_seen_ms.map_or(true, |last| t_ms >= last) {
                collector.last_seen_ms = Some(t_ms);
                collector.last_bpm = Some(sample.bpm);
                latest_changed = true;
            }
        }
        if rejected_future_samples > 0 {
            tracing::warn!(
                collector_id = %collector.collector_id,
                display_name = %collector.display_name,
                seq = payload_seq,
                rejected_future_samples,
                max_future_offset_ms,
                sent_at_ms = payload.sent_at_ms,
                recv_ms,
                "rejected heart-rate samples with future timestamps"
            );
        }

        collector.seen_seqs.insert(payload_seq);
        collector.seq_order.push_back(payload_seq);
        while collector.seq_order.len() > 512 {
            if let Some(old) = collector.seq_order.pop_front() {
                collector.seen_seqs.remove(&old);
            }
        }
        if let Some(name) = payload.display_name {
            collector.display_name = truncate(&name, 80);
        }
        if let Some(model) = payload.device_model {
            collector.device_model = truncate(&model, 80);
        }
        collector.updated_at_ms = Some(recv_ms);
        trim_series(&mut collector.series, recv_ms);

        if accepted > 0 && latest_changed {
            event_display_name = Some(collector.display_name.clone());
        }
        if accepted > 0 {
            persist_collector = Some(collector.clone());
            persist_recv_ms = recv_ms;
        }
        accepted
    };

    if let Some(collector) = persist_collector {
        state
            .db
            .persist_ingest(&collector, payload_seq, &persist_samples, persist_recv_ms)
            .await?;
    }

    if let Some(display_name) = event_display_name {
        let store = state.inner.read().await;
        if let Some(participant) = aggregate_participant_for_name(&store, &display_name, now_ms()) {
            let _ = state
                .events
                .send(LobbyEvent::ParticipantUpdate { participant });
        }
    }

    Ok(Json(IngestResponse {
        ok: true,
        accepted,
        server_time_ms: now_ms(),
        next_policy: None,
    }))
}

async fn lobby_participants(State(state): State<AppState>) -> Json<LobbyResponse> {
    let current_ms = now_ms();
    let store = state.inner.read().await;
    let participants = aggregate_participants(&store, current_ms);
    Json(LobbyResponse {
        server_time_ms: current_ms,
        participants,
    })
}

async fn lobby_events(
    State(state): State<AppState>,
) -> Sse<impl futures_util::Stream<Item = Result<Event, std::convert::Infallible>>> {
    let mut receiver = state.events.subscribe();
    let snapshot = {
        let current_ms = now_ms();
        let store = state.inner.read().await;
        LobbyEvent::Snapshot {
            server_time_ms: current_ms,
            participants: aggregate_participants(&store, current_ms),
        }
    };

    let stream = async_stream::stream! {
        yield Ok(Event::default().json_data(snapshot).expect("serializable event"));
        loop {
            match receiver.recv().await {
                Ok(event) => yield Ok(Event::default().json_data(event).expect("serializable event")),
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    };

    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(10))
            .text("keep-alive"),
    )
}

async fn participant_series(
    State(state): State<AppState>,
    AxumPath(collector_id): AxumPath<String>,
    Query(query): Query<SeriesQuery>,
) -> Result<Json<SeriesResponse>, ApiError> {
    let window_seconds = query.window_seconds.clamp(60, 24 * 3600);
    let max_points = query.max_points.clamp(60, 1200);
    let window_ms = window_seconds * 1000;
    let current_ms = now_ms();
    let cutoff = current_ms - window_ms;
    let upper = current_ms + MAX_SAMPLE_FUTURE_MS;
    let store = state.inner.read().await;
    if !store.collectors.contains_key(&collector_id) {
        return Err(ApiError::NotFound);
    }
    drop(store);
    let samples = state
        .db
        .series(&collector_id, cutoff, upper, window_ms, max_points)
        .await?;
    Ok(Json(SeriesResponse {
        collector_id,
        window_seconds,
        samples,
    }))
}

fn decode_payload(headers: &HeaderMap, bytes: &[u8]) -> Result<BatchPayload, ApiError> {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/cbor")
        .split(';')
        .next()
        .unwrap_or("")
        .trim();

    match content_type {
        "application/cbor" => {
            serde_cbor::from_slice(bytes).map_err(|_| ApiError::BadRequest("invalid cbor"))
        }
        "application/json" => {
            serde_json::from_slice(bytes).map_err(|_| ApiError::BadRequest("invalid json"))
        }
        _ => Err(ApiError::UnsupportedMediaType),
    }
}

fn participant_for(collector: &Collector, current_ms: i64) -> Participant {
    let display_last_seen_ms =
        effective_last_seen_ms(collector.last_seen_ms, collector.updated_at_ms, current_ms);
    Participant {
        collector_id: collector.collector_id.clone(),
        display_name: collector.display_name.clone(),
        device_model: collector.device_model.clone(),
        status: status_for(display_last_seen_ms, current_ms),
        last_bpm: collector.last_bpm,
        last_seen_ms: display_last_seen_ms,
        updated_at_ms: collector.updated_at_ms,
    }
}

fn effective_last_seen_ms(
    last_seen_ms: Option<i64>,
    updated_at_ms: Option<i64>,
    current_ms: i64,
) -> Option<i64> {
    let last_seen_ms = last_seen_ms?;
    if last_seen_ms > current_ms + MAX_SAMPLE_FUTURE_MS {
        return updated_at_ms.map(|updated| updated.min(current_ms));
    }
    Some(last_seen_ms.min(current_ms))
}

fn reconcile_loaded_latest(store: &mut Store, current_ms: i64) {
    let upper_ms = current_ms + MAX_SAMPLE_FUTURE_MS;
    for collector in store.collectors.values_mut() {
        let has_future_latest = collector
            .last_seen_ms
            .is_some_and(|last_seen| last_seen > upper_ms);
        if !has_future_latest {
            continue;
        }
        if let Some(latest) = collector
            .series
            .iter()
            .filter(|sample| sample.t_ms <= upper_ms)
            .max_by_key(|sample| sample.t_ms)
        {
            collector.last_seen_ms = Some(latest.t_ms);
            collector.last_bpm = Some(latest.bpm);
        }
    }
}

fn aggregate_participants(store: &Store, current_ms: i64) -> Vec<Participant> {
    let mut by_name: HashMap<String, Participant> = HashMap::new();
    for collector in store
        .collectors
        .values()
        .filter(|collector| collector.last_seen_ms.is_some())
    {
        let participant = participant_for(collector, current_ms);
        by_name
            .entry(participant.display_name.clone())
            .and_modify(|current| {
                if participant_is_newer(&participant, current) {
                    *current = participant.clone();
                }
            })
            .or_insert(participant);
    }
    let mut participants: Vec<_> = by_name.into_values().collect();
    participants.sort_by(|left, right| {
        status_rank(left.status)
            .cmp(&status_rank(right.status))
            .then_with(|| left.display_name.cmp(&right.display_name))
    });
    participants
}

fn aggregate_participant_for_name(
    store: &Store,
    display_name: &str,
    current_ms: i64,
) -> Option<Participant> {
    store
        .collectors
        .values()
        .filter(|collector| {
            collector.last_seen_ms.is_some() && collector.display_name == display_name
        })
        .map(|collector| participant_for(collector, current_ms))
        .max_by(|left, right| participant_sort_key(left).cmp(&participant_sort_key(right)))
}

fn participant_is_newer(candidate: &Participant, current: &Participant) -> bool {
    participant_sort_key(candidate) > participant_sort_key(current)
}

fn participant_sort_key(participant: &Participant) -> (i64, i64) {
    (
        participant.last_seen_ms.unwrap_or_default(),
        participant.updated_at_ms.unwrap_or_default(),
    )
}

fn merge_latest_sample(samples: &mut Vec<SeriesSample>, latest: SeriesSample) {
    if let Some(existing) = samples.iter_mut().find(|sample| sample.t_ms == latest.t_ms) {
        *existing = latest;
    } else {
        samples.push(latest);
        samples.sort_by_key(|sample| sample.t_ms);
    }
}

fn status_rank(status: ParticipantStatus) -> u8 {
    match status {
        ParticipantStatus::Online => 0,
        ParticipantStatus::Stale => 1,
        ParticipantStatus::Offline => 2,
    }
}

fn status_for(last_seen_ms: Option<i64>, current_ms: i64) -> ParticipantStatus {
    let Some(last_seen_ms) = last_seen_ms else {
        return ParticipantStatus::Offline;
    };
    let age_ms = current_ms - last_seen_ms;
    if age_ms <= ONLINE_AFTER_MS {
        ParticipantStatus::Online
    } else if age_ms <= OFFLINE_AFTER_MS {
        ParticipantStatus::Stale
    } else {
        ParticipantStatus::Offline
    }
}

fn trim_series(series: &mut VecDeque<SeriesSample>, current_ms: i64) {
    let cutoff = current_ms - MEMORY_SERIES_TTL_MS;
    while series.front().is_some_and(|sample| sample.t_ms < cutoff) {
        series.pop_front();
    }
}

fn public_base_url(headers: &HeaderMap) -> String {
    if let Some(value) = headers
        .get("x-forwarded-host")
        .and_then(|value| value.to_str().ok())
    {
        let proto = headers
            .get("x-forwarded-proto")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("https");
        return format!("{proto}://{value}");
    }
    if let Some(value) = headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
    {
        return format!("http://{value}");
    }
    "http://127.0.0.1:8000".to_string()
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
        .map(str::trim)
        .filter(|token| !token.is_empty())
}

fn truncate(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

fn unknown() -> String {
    "Unknown".to_string()
}

fn zero_version() -> String {
    "0.0.0".to_string()
}

fn default_window() -> i64 {
    300
}

fn default_series_max_points() -> i64 {
    DEFAULT_SERIES_MAX_POINTS
}

fn rollup_bucket(t_ms: i64) -> i64 {
    t_ms.div_euclid(ROLLUP_BUCKET_MS) * ROLLUP_BUCKET_MS
}

fn start_retention_sweeper(db: Db) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(RETENTION_SWEEP_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            if let Err(error) = db.prune_expired(now_ms()).await {
                tracing::warn!(?error, "retention sweep failed");
            }
        }
    });
}

fn ignore_duplicate_column(
    result: Result<sqlx::sqlite::SqliteQueryResult, sqlx::Error>,
) -> anyhow::Result<()> {
    match result {
        Ok(_) => Ok(()),
        Err(error) => {
            let message = error.to_string().to_ascii_lowercase();
            if message.contains("duplicate column") {
                Ok(())
            } else {
                Err(error.into())
            }
        }
    }
}

#[derive(Debug)]
enum ApiError {
    BadRequest(&'static str),
    Unauthorized,
    UnsupportedMediaType,
    NotFound,
    Internal,
}

impl From<anyhow::Error> for ApiError {
    fn from(error: anyhow::Error) -> Self {
        tracing::error!(?error, "database operation failed");
        ApiError::Internal
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        match self {
            ApiError::BadRequest(detail) => (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "detail": detail })),
            )
                .into_response(),
            ApiError::Unauthorized => (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "detail": "unauthorized" })),
            )
                .into_response(),
            ApiError::UnsupportedMediaType => (
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                Json(serde_json::json!({ "detail": "use application/cbor" })),
            )
                .into_response(),
            ApiError::NotFound => (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "detail": "not found" })),
            )
                .into_response(),
            ApiError::Internal => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "detail": "internal server error" })),
            )
                .into_response(),
        }
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };
    ctrl_c.await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    async fn test_app() -> Router {
        test_app_with_url().await.0
    }

    async fn test_app_with_url() -> (Router, String) {
        let dir = std::env::current_dir()
            .unwrap()
            .join("target")
            .join("test-dbs");
        std::fs::create_dir_all(&dir).unwrap();
        let db_path = dir.join(format!("{}.db", Uuid::new_v4().simple()));
        let database_url = format!("sqlite://{}", db_path.display());
        let app = app_with_database(&database_url).await.unwrap();
        (app, database_url)
    }

    async fn create_test_session(app: Router) -> SessionResponse {
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/collector/sessions")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"display_name":"Allen","device_model":"Mi Band","client_platform":"android","app_version":"test"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    async fn post_batch(
        app: Router,
        session: &SessionResponse,
        payload: &BatchPayload,
    ) -> IngestResponse {
        let body = serde_cbor::to_vec(payload).unwrap();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/hr/batches")
                    .header(header::CONTENT_TYPE, "application/cbor")
                    .header(
                        header::AUTHORIZATION,
                        format!("Bearer {}", session.collector_token),
                    )
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn session_ingest_and_lobby_work() {
        let app = test_app().await;
        let session = create_test_session(app.clone()).await;

        let payload = BatchPayload {
            schema: 1,
            collector_id: session.collector_id.clone(),
            seq: 1,
            sent_at_ms: now_ms(),
            display_name: Some("Allen".to_string()),
            device_model: Some("Mi Band".to_string()),
            samples: vec![RelativeSample { dt_ms: 0, bpm: 82 }],
            ble: None,
        };
        let ingest = post_batch(app.clone(), &session, &payload).await;
        assert_eq!(ingest.accepted, 1);

        let duplicate = post_batch(app.clone(), &session, &payload).await;
        assert_eq!(duplicate.accepted, 0);

        let lobby_response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/v1/lobby/participants")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let bytes = lobby_response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes();
        let lobby: LobbyResponse = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(lobby.participants.len(), 1);
    }

    #[tokio::test]
    async fn same_display_name_reuses_collector_session() {
        let app = test_app().await;
        let first_session = create_test_session(app.clone()).await;

        let first_payload = BatchPayload {
            schema: 1,
            collector_id: first_session.collector_id.clone(),
            seq: 1,
            sent_at_ms: now_ms(),
            display_name: Some("Allen".to_string()),
            device_model: Some("Mi Band".to_string()),
            samples: vec![RelativeSample { dt_ms: 0, bpm: 82 }],
            ble: None,
        };
        assert_eq!(
            post_batch(app.clone(), &first_session, &first_payload)
                .await
                .accepted,
            1
        );

        let second_session = create_test_session(app.clone()).await;
        assert_eq!(second_session.collector_id, first_session.collector_id);
        assert_eq!(
            second_session.collector_token,
            first_session.collector_token
        );

        let second_payload = BatchPayload {
            schema: 1,
            collector_id: second_session.collector_id.clone(),
            seq: 1,
            sent_at_ms: now_ms(),
            display_name: Some("Allen".to_string()),
            device_model: Some("Mi Band".to_string()),
            samples: vec![RelativeSample { dt_ms: 0, bpm: 88 }],
            ble: None,
        };
        assert_eq!(
            post_batch(app.clone(), &second_session, &second_payload)
                .await
                .accepted,
            1
        );

        let lobby_response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/v1/lobby/participants")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let bytes = lobby_response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes();
        let lobby: LobbyResponse = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(lobby.participants.len(), 1);
        assert_eq!(lobby.participants[0].last_bpm, Some(88));
    }

    #[tokio::test]
    async fn ingest_filters_invalid_samples_and_preserves_newer_latest() {
        let app = test_app().await;
        let session = create_test_session(app.clone()).await;
        let current_ms = now_ms();

        let first = BatchPayload {
            schema: 1,
            collector_id: session.collector_id.clone(),
            seq: 1,
            sent_at_ms: current_ms,
            display_name: Some("Allen".to_string()),
            device_model: Some("Mi Band".to_string()),
            samples: vec![
                RelativeSample { dt_ms: 0, bpm: 20 },
                RelativeSample { dt_ms: 0, bpm: 90 },
                RelativeSample { dt_ms: 0, bpm: 241 },
            ],
            ble: None,
        };
        assert_eq!(post_batch(app.clone(), &session, &first).await.accepted, 1);

        let older = BatchPayload {
            schema: 1,
            collector_id: session.collector_id.clone(),
            seq: 2,
            sent_at_ms: current_ms - 10_000,
            display_name: None,
            device_model: None,
            samples: vec![RelativeSample { dt_ms: 0, bpm: 50 }],
            ble: None,
        };
        assert_eq!(post_batch(app.clone(), &session, &older).await.accepted, 1);

        let lobby_response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/v1/lobby/participants")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let bytes = lobby_response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes();
        let lobby: LobbyResponse = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(lobby.participants[0].last_bpm, Some(90));
    }

    #[tokio::test]
    async fn series_preserves_exact_latest_sample_after_bucket_aggregation() {
        let app = test_app().await;
        let session = create_test_session(app.clone()).await;
        let current_ms = now_ms();
        let payload = BatchPayload {
            schema: 1,
            collector_id: session.collector_id.clone(),
            seq: 1,
            sent_at_ms: current_ms,
            display_name: Some("Allen".to_string()),
            device_model: Some("Mi Band".to_string()),
            samples: vec![
                RelativeSample {
                    dt_ms: -100,
                    bpm: 80,
                },
                RelativeSample { dt_ms: 0, bpm: 100 },
            ],
            ble: None,
        };
        assert_eq!(
            post_batch(app.clone(), &session, &payload).await.accepted,
            2
        );

        let series_response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!(
                        "/api/v1/participants/{}/series?window_seconds=600&max_points=600",
                        session.collector_id
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let bytes = series_response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes();
        let series: SeriesResponse = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(series.samples.last().map(|sample| sample.bpm), Some(100));
        assert_eq!(
            series.samples.last().map(|sample| sample.t_ms),
            Some(current_ms)
        );
    }

    #[tokio::test]
    async fn future_sample_times_are_rejected() {
        let app = test_app().await;
        let session = create_test_session(app.clone()).await;
        let current_ms = now_ms();
        let payload = BatchPayload {
            schema: 1,
            collector_id: session.collector_id.clone(),
            seq: 1,
            sent_at_ms: current_ms,
            display_name: Some("Allen".to_string()),
            device_model: Some("Mi Band".to_string()),
            samples: vec![RelativeSample {
                dt_ms: 6 * 60 * 60_000,
                bpm: 88,
            }],
            ble: None,
        };
        assert_eq!(
            post_batch(app.clone(), &session, &payload).await.accepted,
            0
        );

        let lobby_response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/v1/lobby/participants")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let bytes = lobby_response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes();
        let lobby: LobbyResponse = serde_json::from_slice(&bytes).unwrap();
        assert!(lobby.participants.is_empty());
    }

    #[tokio::test]
    async fn database_persists_lobby_and_series_across_restart() {
        let (app, database_url) = test_app_with_url().await;
        let session = create_test_session(app.clone()).await;
        let current_ms = now_ms();
        let payload = BatchPayload {
            schema: 1,
            collector_id: session.collector_id.clone(),
            seq: 1,
            sent_at_ms: current_ms,
            display_name: Some("Allen".to_string()),
            device_model: Some("Mi Band".to_string()),
            samples: vec![
                RelativeSample {
                    dt_ms: -1_000,
                    bpm: 80,
                },
                RelativeSample { dt_ms: 0, bpm: 82 },
            ],
            ble: None,
        };
        assert_eq!(post_batch(app, &session, &payload).await.accepted, 2);

        let restarted = app_with_database(&database_url).await.unwrap();
        let lobby_response = restarted
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/v1/lobby/participants")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let bytes = lobby_response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes();
        let lobby: LobbyResponse = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(lobby.participants.len(), 1);
        assert_eq!(lobby.participants[0].last_bpm, Some(82));

        let series_response = restarted
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!(
                        "/api/v1/participants/{}/series?window_seconds=600",
                        session.collector_id
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let bytes = series_response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes();
        let series: SeriesResponse = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(series.samples.len(), 2);
        assert_eq!(series.samples[1].bpm, 82);
    }

    #[tokio::test]
    async fn startup_backfills_rollups_before_pruning_expired_raw_samples() {
        let (app, database_url) = test_app_with_url().await;
        let session = create_test_session(app.clone()).await;
        drop(app);

        let old_ms = now_ms() - 25 * 60 * 60_000;
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await
            .unwrap();
        sqlx::query(
            r#"
            INSERT INTO heart_rate_samples (collector_id, t_ms, bpm, seq, received_at_ms)
            VALUES (?, ?, ?, ?, ?), (?, ?, ?, ?, ?)
            "#,
        )
        .bind(&session.collector_id)
        .bind(old_ms)
        .bind(90)
        .bind(9001_i64)
        .bind(old_ms)
        .bind(&session.collector_id)
        .bind(old_ms + 1_000)
        .bind(100)
        .bind(9001_i64)
        .bind(old_ms + 1_000)
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;

        let restarted = app_with_database(&database_url).await.unwrap();
        drop(restarted);

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await
            .unwrap();
        let raw_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM heart_rate_samples WHERE collector_id = ?")
                .bind(&session.collector_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(raw_count, 0);

        let rollup = sqlx::query(
            r#"
            SELECT sample_count, bpm_sum, bpm_sum_sq, min_bpm, max_bpm
            FROM heart_rate_rollups
            WHERE collector_id = ?
            "#,
        )
        .bind(&session.collector_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(rollup.get::<i64, _>("sample_count"), 2);
        assert_eq!(rollup.get::<i64, _>("bpm_sum"), 190);
        assert_eq!(rollup.get::<i64, _>("bpm_sum_sq"), 18_100);
        assert_eq!(rollup.get::<i32, _>("min_bpm"), 90);
        assert_eq!(rollup.get::<i32, _>("max_bpm"), 100);
    }

    #[test]
    fn participant_status_boundaries_match_lobby_contract() {
        let current_ms = 1_000_000;
        assert!(matches!(
            status_for(Some(current_ms - 20_000), current_ms),
            ParticipantStatus::Online
        ));
        assert!(matches!(
            status_for(Some(current_ms - 20_001), current_ms),
            ParticipantStatus::Stale
        ));
        assert!(matches!(
            status_for(Some(current_ms - 120_001), current_ms),
            ParticipantStatus::Offline
        ));
    }

    #[test]
    fn participant_display_time_uses_receive_time_for_future_dirty_samples() {
        let current_ms = 1_000_000;
        assert_eq!(
            effective_last_seen_ms(
                Some(current_ms + 60 * 60_000),
                Some(current_ms - 30_000),
                current_ms,
            ),
            Some(current_ms - 30_000)
        );
        assert_eq!(
            effective_last_seen_ms(
                Some(current_ms + 1_000),
                Some(current_ms - 30_000),
                current_ms
            ),
            Some(current_ms)
        );
    }

    #[test]
    fn loaded_future_latest_is_reconciled_from_valid_recent_series() {
        let current_ms = 1_000_000;
        let mut store = Store::default();
        store.collectors.insert(
            "collector".to_string(),
            Collector {
                collector_id: "collector".to_string(),
                display_name: "Allen".to_string(),
                device_model: "Mi Band".to_string(),
                _client_platform: "test".to_string(),
                _app_version: "test".to_string(),
                _created_at_ms: current_ms,
                last_bpm: Some(90),
                last_seen_ms: Some(current_ms + 60 * 60_000),
                updated_at_ms: Some(current_ms - 60_000),
                seen_seqs: HashSet::new(),
                seq_order: VecDeque::new(),
                series: VecDeque::from(vec![SeriesSample {
                    t_ms: current_ms - 10_000,
                    bpm: 104,
                }]),
            },
        );

        reconcile_loaded_latest(&mut store, current_ms);

        let collector = store.collectors.get("collector").unwrap();
        assert_eq!(collector.last_bpm, Some(104));
        assert_eq!(collector.last_seen_ms, Some(current_ms - 10_000));
    }
}
