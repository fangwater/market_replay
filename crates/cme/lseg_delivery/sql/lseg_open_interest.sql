-- Open interest from LSEG TAS (`normalised`) source gzips; written only by `tas_open_interest`.
-- See docs/lseg/open_interest.md. Idempotent: safe to apply again.
CREATE SCHEMA IF NOT EXISTS lseg_open_interest;

CREATE TABLE IF NOT EXISTS lseg_open_interest.schema_metadata (
    key text PRIMARY KEY,
    value text NOT NULL
);
INSERT INTO lseg_open_interest.schema_metadata (key, value)
VALUES ('schema_version', '1'), ('rule_version', 'lseg_oi_v1')
ON CONFLICT (key) DO NOTHING;

-- One row per scanned source gzip. A source is loaded in one transaction together with its rows.
CREATE TABLE IF NOT EXISTS lseg_open_interest.sources (
    source_path text PRIMARY KEY,
    source_kind text NOT NULL CHECK (source_kind IN ('period', 'daily')),
    -- package directory name (period) or product directory (daily)
    package text NOT NULL CHECK (package <> ''),
    source_bytes bigint NOT NULL CHECK (source_bytes > 0),
    source_mtime_ns bigint NOT NULL,
    gzip_members bigint NOT NULL CHECK (gzip_members > 0),
    data_rows bigint NOT NULL CHECK (data_rows >= 0),
    quoted_rows bigint NOT NULL CHECK (quoted_rows >= 0),
    oi_rows bigint NOT NULL CHECK (oi_rows >= 0),
    observation_rows bigint NOT NULL CHECK (observation_rows >= 0),
    reject_rows bigint NOT NULL CHECK (reject_rows >= 0),
    min_published_ns bigint,
    max_published_ns bigint,
    rule_version text NOT NULL,
    tool_version text NOT NULL,
    scan_seconds double precision NOT NULL,
    loaded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (oi_rows >= observation_rows + reject_rows),
    CHECK (min_published_ns IS NULL OR max_published_ns >= min_published_ns)
);

-- Consecutive source rows of one RIC with the same (oi_date, open_interest, row_type) folded
-- into one row. The value known at time t is the row with the latest first_published_ns <= t.
CREATE TABLE IF NOT EXISTS lseg_open_interest.observations (
    ric text NOT NULL CHECK (ric <> ''),
    -- source `Date`: the trade date the figure belongs to; NULL when the source row has none
    -- (e.g. 2010). Never filled in from the publication time.
    oi_date date,
    open_interest bigint NOT NULL CHECK (open_interest >= 0),
    row_type text NOT NULL,
    first_published_ns bigint NOT NULL,
    last_published_ns bigint NOT NULL,
    publication_rows integer NOT NULL CHECK (publication_rows > 0),
    source_path text NOT NULL REFERENCES lseg_open_interest.sources (source_path) ON DELETE CASCADE,
    CHECK (last_published_ns >= first_published_ns)
);
CREATE INDEX IF NOT EXISTS observations_ric_date_idx
    ON lseg_open_interest.observations (ric, oi_date, first_published_ns);
CREATE INDEX IF NOT EXISTS observations_ric_published_idx
    ON lseg_open_interest.observations (ric, first_published_ns);
CREATE INDEX IF NOT EXISTS observations_source_idx
    ON lseg_open_interest.observations (source_path);

-- Rows with a non-empty value that could not be taken as-is; raw text kept, never used.
CREATE TABLE IF NOT EXISTS lseg_open_interest.rejects (
    ric text NOT NULL,
    published_ns bigint,
    raw_value text NOT NULL,
    raw_date text NOT NULL,
    reason text NOT NULL,
    source_path text NOT NULL REFERENCES lseg_open_interest.sources (source_path) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS rejects_source_idx ON lseg_open_interest.rejects (source_path);

-- Per RIC and trade date: the first published figure (preliminary) and the last one (final),
-- each with its publication time. Rows without a source Date are not in this view.
CREATE OR REPLACE VIEW lseg_open_interest.by_trade_date AS
SELECT ric,
       oi_date,
       (array_agg(open_interest ORDER BY first_published_ns))[1] AS first_open_interest,
       min(first_published_ns) AS first_published_ns,
       (array_agg(open_interest ORDER BY first_published_ns DESC))[1] AS last_open_interest,
       max(first_published_ns) AS last_published_ns,
       count(*) AS observations
FROM lseg_open_interest.observations
WHERE oi_date IS NOT NULL
GROUP BY ric, oi_date;
