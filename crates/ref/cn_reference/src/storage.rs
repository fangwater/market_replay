//! PostgreSQL storage for incremental reference records (port of
//! `cn_futures_ref/storage.py`). The schema name is a parameter so tests can
//! use a throwaway schema; production is `cn_futures_reference`.
use crate::json::{parse_ordered, J};
use crate::models::{
    data_error, Record, DB_COLUMNS, SCHEMA_VERSION, SESSION_SOURCE_TYPE, SOURCE_TYPE,
};
use crate::sessions::SessionRecord;
use crate::source::parse_date;
use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDate, NaiveTime, Timelike, Utc};
use sha2::{Digest, Sha256};
use sqlx::pool::PoolConnection;
use sqlx::postgres::{PgPoolOptions, PgRow};
use sqlx::{AssertSqlSafe, Column, PgConnection, PgPool, Postgres, Row, TypeInfo};
use std::collections::HashSet;
use std::str::FromStr;

pub const PRODUCTION_SCHEMA: &str = "cn_futures_reference";
const DDL: &str = include_str!("schema.sql");
const DAILY_LOCK: i64 = 695_102_482;

const SESSION_COLUMNS: [&str; 11] = [
    "venue",
    "product_id",
    "segment_no",
    "time_begin",
    "time_end",
    "crosses_midnight",
    "session_group",
    "product_class",
    "area",
    "is_active",
    "raw_json",
];
/// `SELECT *` order of `continuous_session_versions`, jsonb read as text.
const SESSION_SELECT: &str = "id, venue, product_id, segment_no, time_begin, time_end, \
    crosses_midnight, session_group, product_class, area, is_active, raw_json::text AS raw_json, \
    source_date, start_ts, update_ts, source_url, source_type, record_hash";

#[derive(Clone)]
pub struct Db {
    pub pool: PgPool,
    pub schema: String,
}

pub enum Bind {
    Date(NaiveDate),
    Text(String),
}

#[derive(Debug, Default, Clone, Copy)]
pub struct MergeCounts {
    pub inserted: i64,
    pub confirmed: i64,
    pub deactivated: i64,
    pub total: i64,
}

/// Schema names are validated as `[A-Za-z0-9_]+` in `Db::connect`, and every
/// other value is a bind parameter, so the dynamic SQL text is safe.
fn safe(sql: &str) -> AssertSqlSafe<&str> {
    AssertSqlSafe(sql)
}

fn run_query<'q>(
    sql: &'q str,
    binds: &'q [Bind],
) -> sqlx::query::Query<'q, Postgres, sqlx::postgres::PgArguments> {
    let mut q = sqlx::query(safe(sql));
    for b in binds {
        q = match b {
            Bind::Date(d) => q.bind(*d),
            Bind::Text(s) => q.bind(s.as_str()),
        };
    }
    q
}

fn iso_time(t: NaiveTime) -> String {
    let micro = t.nanosecond() / 1000;
    if micro == 0 {
        t.format("%H:%M:%S").to_string()
    } else {
        format!("{}.{micro:06}", t.format("%H:%M:%S"))
    }
}

fn iso_datetime(t: DateTime<Utc>) -> String {
    format!("{}T{}+00:00", t.date_naive(), iso_time(t.time()))
}

/// Generic `psycopg dict_row` + `row_dict` conversion.
fn row_to_j(row: &PgRow, json_cols: &[&str]) -> Result<J> {
    let mut items = Vec::new();
    for (i, col) in row.columns().iter().enumerate() {
        let name = col.name().to_string();
        let ty = col.type_info().name().to_string();
        let v = match ty.as_str() {
            "TEXT" | "VARCHAR" | "BPCHAR" | "NAME" => {
                let s: Option<String> = row.try_get(i)?;
                match s {
                    Some(t) if json_cols.contains(&name.as_str()) => parse_ordered(&t)?,
                    other => J::opt(other),
                }
            }
            "INT8" => row.try_get::<Option<i64>, _>(i)?.map_or(J::Null, J::Int),
            "INT4" => row
                .try_get::<Option<i32>, _>(i)?
                .map_or(J::Null, |v| J::Int(v.into())),
            "INT2" => row
                .try_get::<Option<i16>, _>(i)?
                .map_or(J::Null, |v| J::Int(v.into())),
            "BOOL" => row.try_get::<Option<bool>, _>(i)?.map_or(J::Null, J::Bool),
            "DATE" => row
                .try_get::<Option<NaiveDate>, _>(i)?
                .map_or(J::Null, |d| J::Str(d.to_string())),
            "TIME" => row
                .try_get::<Option<NaiveTime>, _>(i)?
                .map_or(J::Null, |t| J::Str(iso_time(t))),
            "TIMESTAMPTZ" => row
                .try_get::<Option<DateTime<Utc>>, _>(i)?
                .map_or(J::Null, |t| J::Str(iso_datetime(t))),
            other => anyhow::bail!("unsupported column type {other} for {name}"),
        };
        items.push((name, v));
    }
    Ok(J::Obj(items))
}

fn sha256_hex(s: &str) -> String {
    let d = Sha256::digest(s.as_bytes());
    d.iter().map(|b| format!("{b:02x}")).collect()
}

/// Recursively sorts object keys (`json.dumps(sort_keys=True)`).
fn sorted(j: &J) -> J {
    match j {
        J::Obj(items) => {
            let mut v: Vec<(String, J)> =
                items.iter().map(|(k, v)| (k.clone(), sorted(v))).collect();
            v.sort_by(|a, b| a.0.cmp(&b.0));
            J::Obj(v)
        }
        J::Arr(items) => J::Arr(items.iter().map(sorted).collect()),
        other => other.clone(),
    }
}

pub fn record_hash(record: &Record) -> String {
    sha256_hex(&record.hash_payload())
}

pub fn session_hash(r: &SessionRecord) -> String {
    let payload = J::Arr(vec![
        J::s(&r.venue),
        J::s(&r.product_id),
        J::Int(r.segment_no),
        J::s(&r.time_begin),
        J::s(&r.time_end),
        J::Bool(r.crosses_midnight),
        J::s(&r.session_group),
        J::s(&r.product_class),
        J::s(&r.area),
        J::Bool(r.is_active),
        r.raw_json.clone(),
    ]);
    sha256_hex(&sorted(&payload).dumps(false))
}

impl Db {
    /// Connects and makes sure the schema exists (DDL only when missing or
    /// older; production reads never rewrite an up-to-date schema).
    pub async fn connect(url: &str, schema: &str) -> Result<Db> {
        anyhow::ensure!(
            schema
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_')
                && !schema.is_empty(),
            "invalid schema name"
        );
        let opts = sqlx::postgres::PgConnectOptions::from_str(url)
            .context("CN_FUTURES_REF_DATABASE_URL is not a PostgreSQL URL")?;
        let pool = PgPoolOptions::new()
            .max_connections(6)
            .acquire_timeout(std::time::Duration::from_secs(30))
            .connect_with(opts)
            .await
            .map_err(|e| {
                crate::models::DataError(format!("could not connect to PostgreSQL: {e}"))
            })?;
        let db = Db {
            pool,
            schema: schema.to_string(),
        };
        db.ensure_schema().await?;
        Ok(db)
    }

    fn q(&self, sql: &str) -> String {
        sql.replace("{S}", &self.schema)
    }

    async fn ensure_schema(&self) -> Result<()> {
        let present: Option<String> = sqlx::query_scalar("SELECT to_regclass($1)::text")
            .bind(format!("{}.schema_metadata", self.schema))
            .fetch_one(&self.pool)
            .await?;
        if present.is_some() {
            let version: Option<String> = sqlx::query_scalar(safe(
                &self.q("SELECT value FROM {S}.schema_metadata WHERE key = 'schema_version'"),
            ))
            .fetch_optional(&self.pool)
            .await?;
            if let Some(v) = version.as_deref().and_then(|v| v.parse::<i64>().ok()) {
                if v > SCHEMA_VERSION {
                    return data_error(format!("database schema version {v} is unsupported"));
                }
                if v == SCHEMA_VERSION {
                    return Ok(());
                }
            }
        }
        sqlx::raw_sql(safe(&format!(
            "CREATE SCHEMA IF NOT EXISTS {} AUTHORIZATION CURRENT_USER",
            self.schema
        )))
        .execute(&self.pool)
        .await?;
        sqlx::raw_sql(safe(
            &DDL.replace("cn_futures_reference.", &format!("{}.", self.schema)),
        ))
        .execute(&self.pool)
        .await?;
        sqlx::query(safe(&self.q(
            "INSERT INTO {S}.schema_metadata(key, value) VALUES('schema_version', $1) \
             ON CONFLICT(key) DO UPDATE SET value = EXCLUDED.value",
        )))
        .bind(SCHEMA_VERSION.to_string())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    // ------------------------------------------------------------ reads

    pub async fn data_state(&self, as_of: Option<&str>) -> Result<J> {
        let mut conn = self.pool.acquire().await?;
        self.data_state_on(&mut conn, as_of).await
    }

    pub async fn data_state_on(&self, conn: &mut PgConnection, as_of: Option<&str>) -> Result<J> {
        let date = as_of.map(|a| parse_date(a, "as-of")).transpose()?;
        let (clause, binds) = match date {
            Some(d) => ("WHERE source_date <= $1", vec![Bind::Date(d)]),
            None => ("", vec![]),
        };
        let sql = self.q(&format!(
            "WITH current_records AS (\
               SELECT DISTINCT ON (venue, instrument_id) * FROM {{S}}.instrument_versions {clause} \
               ORDER BY venue, instrument_id, source_date DESC, start_ts DESC, id DESC) \
             SELECT MAX(source_date)::TEXT AS as_of, MAX(start_ts)::TEXT AS latest_start_ts, \
                    MAX(update_ts)::TEXT AS latest_update_ts, COUNT(*) AS instrument_count, \
                    COALESCE((SELECT COUNT(*) FROM {{S}}.instrument_versions), 0) AS version_count \
             FROM current_records"
        ));
        let row = run_query(&sql, &binds).fetch_one(&mut *conn).await?;
        let state = row_to_j(&row, &[])?;
        if matches!(state.get("as_of"), Some(J::Null) | None) {
            return data_error("no usable incremental records found; import or sync data first");
        }
        Ok(state)
    }

    pub async fn current_records(
        &self,
        conn: &mut PgConnection,
        as_of: Option<&str>,
        venue: Option<&str>,
        product: Option<&str>,
        instrument: Option<&str>,
        include_options: bool,
    ) -> Result<Vec<J>> {
        let date = as_of.map(|a| parse_date(a, "as-of")).transpose()?;
        let mut binds: Vec<Bind> = Vec::new();
        let mut version_where = String::new();
        if let Some(d) = date {
            binds.push(Bind::Date(d));
            version_where = format!("WHERE source_date <= ${}", binds.len());
        }
        let mut filters: Vec<String> = Vec::new();
        for (col, value) in [
            ("venue", venue),
            ("product_id", product),
            ("instrument_id", instrument),
        ] {
            if let Some(v) = value {
                binds.push(Bind::Text(v.to_string()));
                filters.push(format!("UPPER({col}) = UPPER(${})", binds.len()));
            }
        }
        if !include_options {
            filters.push("instrument_type = 'FUTURE'".into());
        }
        let record_filter = if filters.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", filters.join(" AND "))
        };
        let sql = self.q(&format!(
            "WITH current_records AS (\
               SELECT DISTINCT ON (venue, instrument_id) * FROM {{S}}.instrument_versions {version_where} \
               ORDER BY venue, instrument_id, source_date DESC, start_ts DESC, id DESC) \
             SELECT * FROM current_records {record_filter} ORDER BY venue, product_id, instrument_id"
        ));
        let rows = run_query(&sql, &binds).fetch_all(&mut *conn).await?;
        rows.iter().map(|r| row_to_j(r, &[])).collect()
    }

    /// Overrides in effect on `effective`, in Python's per-record order
    /// (instrument-specific after product-wide, then effective_from, id).
    pub async fn overrides_effective(
        &self,
        conn: &mut PgConnection,
        effective: NaiveDate,
    ) -> Result<Vec<J>> {
        let sql = self.q(
            "SELECT * FROM {S}.official_overrides WHERE effective_from <= $1 \
             AND (effective_to IS NULL OR effective_to > $1) \
             ORDER BY CASE WHEN instrument_id = '' THEN 0 ELSE 1 END, effective_from, id",
        );
        let rows = sqlx::query(safe(&sql))
            .bind(effective)
            .fetch_all(&mut *conn)
            .await?;
        rows.iter().map(|r| row_to_j(r, &[])).collect()
    }

    fn session_state_sql(&self, with_date: bool) -> String {
        let clause = if with_date {
            "WHERE source_date <= $1"
        } else {
            ""
        };
        self.q(&format!(
            "WITH current_sessions AS (\
               SELECT DISTINCT ON (venue, product_id, segment_no) * FROM {{S}}.continuous_session_versions {clause} \
               ORDER BY venue, product_id, segment_no, source_date DESC, start_ts DESC, id DESC) \
             SELECT MAX(source_date)::TEXT AS as_of, MAX(start_ts)::TEXT AS latest_start_ts, \
                    MAX(update_ts)::TEXT AS latest_update_ts, \
                    COUNT(*) FILTER (WHERE is_active) AS session_count, \
                    COUNT(DISTINCT (venue, product_id)) FILTER (WHERE is_active) AS product_count, \
                    COUNT(DISTINCT venue) FILTER (WHERE is_active) AS venue_count, \
                    (SELECT COUNT(*) FROM {{S}}.continuous_session_versions) AS version_count \
             FROM current_sessions"
        ))
    }

    pub async fn continuous_session_state(
        &self,
        conn: &mut PgConnection,
        as_of: Option<&str>,
    ) -> Result<J> {
        let date = as_of.map(|a| parse_date(a, "as-of")).transpose()?;
        let sql = self.session_state_sql(date.is_some());
        let binds: Vec<Bind> = date.map(Bind::Date).into_iter().collect();
        let row = run_query(&sql, &binds).fetch_one(&mut *conn).await?;
        let state = row_to_j(&row, &[])?;
        if matches!(state.get("as_of"), Some(J::Null) | None) {
            return data_error("no continuous-session records found; sync session data first");
        }
        Ok(state)
    }

    pub async fn current_continuous_sessions(
        &self,
        conn: &mut PgConnection,
        as_of: Option<&str>,
        venue: Option<&str>,
        product: Option<&str>,
        include_inactive: bool,
    ) -> Result<Vec<J>> {
        let date = as_of.map(|a| parse_date(a, "as-of")).transpose()?;
        let mut binds: Vec<Bind> = Vec::new();
        let mut version_where = String::new();
        if let Some(d) = date {
            binds.push(Bind::Date(d));
            version_where = format!("WHERE source_date <= ${}", binds.len());
        }
        let mut filters: Vec<String> = Vec::new();
        for (col, value) in [("venue", venue), ("product_id", product)] {
            if let Some(v) = value {
                binds.push(Bind::Text(v.to_string()));
                filters.push(format!("UPPER({col}) = UPPER(${})", binds.len()));
            }
        }
        if !include_inactive {
            filters.push("is_active".into());
        }
        let row_filter = if filters.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", filters.join(" AND "))
        };
        let sql = self.q(&format!(
            "WITH current_sessions AS (\
               SELECT DISTINCT ON (venue, product_id, segment_no) * FROM {{S}}.continuous_session_versions {version_where} \
               ORDER BY venue, product_id, segment_no, source_date DESC, start_ts DESC, id DESC) \
             SELECT {SESSION_SELECT} FROM current_sessions {row_filter} \
             ORDER BY venue, product_id, segment_no"
        ));
        let rows = run_query(&sql, &binds).fetch_all(&mut *conn).await?;
        rows.iter().map(|r| row_to_j(r, &["raw_json"])).collect()
    }

    // ----------------------------------------------------------- writes

    pub async fn merge_records(
        &self,
        records: &[Record],
        source_date: NaiveDate,
        observed_at: DateTime<Utc>,
        source_url: &str,
    ) -> Result<MergeCounts> {
        let mut tx = self.pool.begin().await?;
        let columns: Vec<&str> = DB_COLUMNS
            .iter()
            .copied()
            .chain([
                "source_date",
                "start_ts",
                "update_ts",
                "source_url",
                "source_type",
                "record_hash",
            ])
            .collect();
        let placeholders: Vec<String> = (1..=columns.len()).map(|i| format!("${i}")).collect();
        let insert = self.q(&format!(
            "INSERT INTO {{S}}.instrument_versions({}) VALUES({})",
            columns.join(","),
            placeholders.join(",")
        ));
        let find = self.q("SELECT id, record_hash FROM {S}.instrument_versions \
             WHERE venue = $1 AND instrument_id = $2 AND source_date <= $3 \
             ORDER BY source_date DESC, start_ts DESC, id DESC LIMIT 1");
        let touch = self.q(
            "UPDATE {S}.instrument_versions SET update_ts = GREATEST(update_ts, $1) WHERE id = $2",
        );
        let mut counts = MergeCounts {
            total: records.len() as i64,
            ..Default::default()
        };
        for record in records {
            let fingerprint = record_hash(record);
            let current: Option<(i64, String)> = sqlx::query_as(safe(&find))
                .bind(record.get("venue"))
                .bind(record.get("instrument_id"))
                .bind(source_date)
                .fetch_optional(&mut *tx)
                .await?;
            match current {
                Some((id, hash)) if hash == fingerprint => {
                    sqlx::query(safe(&touch))
                        .bind(observed_at)
                        .bind(id)
                        .execute(&mut *tx)
                        .await?;
                    counts.confirmed += 1;
                }
                _ => {
                    let mut q = sqlx::query(safe(&insert));
                    for c in DB_COLUMNS {
                        q = q.bind(record.get(c));
                    }
                    q.bind(source_date)
                        .bind(observed_at)
                        .bind(observed_at)
                        .bind(source_url)
                        .bind(SOURCE_TYPE)
                        .bind(&fingerprint)
                        .execute(&mut *tx)
                        .await?;
                    counts.inserted += 1;
                }
            }
        }
        tx.commit().await?;
        Ok(counts)
    }

    fn session_insert_sql(&self) -> String {
        self.q(&format!(
            "INSERT INTO {{S}}.continuous_session_versions({}, source_date, start_ts, update_ts, \
             source_url, source_type, record_hash) VALUES($1,$2,$3,$4::time,$5::time,$6,$7,$8,$9,$10,\
             $11::jsonb,$12,$13,$14,$15,$16,$17)",
            SESSION_COLUMNS.join(",")
        ))
    }

    async fn insert_session(
        &self,
        conn: &mut PgConnection,
        sql: &str,
        r: &SessionRecord,
        source_date: NaiveDate,
        observed_at: DateTime<Utc>,
        source_url: &str,
        fingerprint: &str,
    ) -> Result<()> {
        let raw = match &r.raw_json {
            J::Str(s) => s.clone(),
            other => other.dumps(false),
        };
        sqlx::query(safe(sql))
            .bind(&r.venue)
            .bind(&r.product_id)
            .bind(r.segment_no as i32)
            .bind(&r.time_begin)
            .bind(&r.time_end)
            .bind(r.crosses_midnight)
            .bind(&r.session_group)
            .bind(&r.product_class)
            .bind(&r.area)
            .bind(r.is_active)
            .bind(raw)
            .bind(source_date)
            .bind(observed_at)
            .bind(observed_at)
            .bind(source_url)
            .bind(SESSION_SOURCE_TYPE)
            .bind(fingerprint)
            .execute(&mut *conn)
            .await?;
        Ok(())
    }

    pub async fn merge_continuous_sessions(
        &self,
        records: &[SessionRecord],
        source_date: NaiveDate,
        observed_at: DateTime<Utc>,
        source_url: &str,
        reconcile: bool,
    ) -> Result<MergeCounts> {
        let mut tx = self.pool.begin().await?;
        let insert = self.session_insert_sql();
        let find = self.q(
            "SELECT id, record_hash FROM {S}.continuous_session_versions \
             WHERE UPPER(venue) = UPPER($1) AND UPPER(product_id) = UPPER($2) \
             AND segment_no = $3 AND source_date <= $4 \
             ORDER BY source_date DESC, start_ts DESC, id DESC LIMIT 1",
        );
        let touch = self.q(
            "UPDATE {S}.continuous_session_versions SET update_ts = GREATEST(update_ts, $1) WHERE id = $2",
        );
        let incoming: HashSet<(String, String, i64)> = records
            .iter()
            .map(|r| {
                (
                    r.venue.to_uppercase(),
                    r.product_id.to_uppercase(),
                    r.segment_no,
                )
            })
            .collect();
        let mut counts = MergeCounts {
            total: records.len() as i64,
            ..Default::default()
        };
        for r in records {
            let fingerprint = session_hash(r);
            let current: Option<(i64, String)> = sqlx::query_as(safe(&find))
                .bind(&r.venue)
                .bind(&r.product_id)
                .bind(r.segment_no as i32)
                .bind(source_date)
                .fetch_optional(&mut *tx)
                .await?;
            match current {
                Some((id, hash)) if hash == fingerprint => {
                    sqlx::query(safe(&touch))
                        .bind(observed_at)
                        .bind(id)
                        .execute(&mut *tx)
                        .await?;
                    counts.confirmed += 1;
                }
                _ => {
                    self.insert_session(
                        &mut tx,
                        &insert,
                        r,
                        source_date,
                        observed_at,
                        source_url,
                        &fingerprint,
                    )
                    .await?;
                    counts.inserted += 1;
                }
            }
        }
        if reconcile {
            let as_of = source_date.to_string();
            let current = self
                .current_continuous_sessions(&mut tx, Some(&as_of), None, None, false)
                .await?;
            for row in current {
                let venue = row.str_of("venue").unwrap_or_default().to_string();
                let product = row.str_of("product_id").unwrap_or_default().to_string();
                let segment = match row.get("segment_no") {
                    Some(J::Int(i)) => *i,
                    _ => continue,
                };
                if incoming.contains(&(venue.to_uppercase(), product.to_uppercase(), segment)) {
                    continue;
                }
                let text = |k: &str| row.str_of(k).unwrap_or_default().to_string();
                let tombstone = SessionRecord {
                    venue,
                    product_id: product,
                    segment_no: segment,
                    time_begin: text("time_begin"),
                    time_end: text("time_end"),
                    crosses_midnight: matches!(row.get("crosses_midnight"), Some(J::Bool(true))),
                    session_group: text("session_group"),
                    product_class: text("product_class"),
                    area: text("area"),
                    is_active: false,
                    raw_json: row.get("raw_json").cloned().unwrap_or(J::Null),
                };
                let fingerprint = session_hash(&tombstone);
                self.insert_session(
                    &mut tx,
                    &insert,
                    &tombstone,
                    source_date,
                    observed_at,
                    source_url,
                    &fingerprint,
                )
                .await?;
                counts.inserted += 1;
                counts.deactivated += 1;
            }
        }
        tx.commit().await?;
        Ok(counts)
    }

    // ------------------------------------------------------------ lock

    /// `pg_try_advisory_lock` on a dedicated connection; `None` when held.
    pub async fn try_daily_lock(&self) -> Result<Option<PoolConnection<Postgres>>> {
        let mut conn = self.pool.acquire().await?;
        let got: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(DAILY_LOCK)
            .fetch_one(&mut *conn)
            .await?;
        Ok(got.then_some(conn))
    }

    pub async fn release_daily_lock(mut conn: PoolConnection<Postgres>) -> Result<()> {
        sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(DAILY_LOCK)
            .execute(&mut *conn)
            .await?;
        Ok(())
    }
}
