BEGIN;

CREATE TABLE IF NOT EXISTS public.cme_tas_action_events (
    product text NOT NULL,
    ric text NOT NULL,
    source_year smallint NOT NULL,
    action text NOT NULL,
    event_second_utc timestamptz NOT NULL,
    event_ts_utc timestamptz NOT NULL,
    event_ts_ns bigint NOT NULL,
    is_inferred boolean NOT NULL,
    mapping_rule text NOT NULL,
    mapping_version text NOT NULL,
    qualifiers text NOT NULL,
    source_event_count integer NOT NULL,
    loaded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (product, ric, action, event_second_utc),
    CHECK (product IN ('ES', 'NQ', 'RTY', 'YM', 'GC', 'CL')),
    CHECK (source_year BETWEEN 2000 AND 2100),
    CHECK (action IN ('CLOSE', 'PRE_OPEN', 'TRADING', 'NEW_PRICE_INDICATION')),
    CHECK (event_second_utc = date_trunc('second', event_second_utc)),
    CHECK (event_ts_utc >= event_second_utc),
    CHECK (event_ts_utc < event_second_utc + interval '1 second'),
    CHECK (mapping_rule <> ''),
    CHECK (mapping_version <> ''),
    CHECK (source_event_count > 0)
);

ALTER TABLE public.cme_tas_action_events
    DROP CONSTRAINT IF EXISTS cme_tas_action_events_check2;

CREATE INDEX IF NOT EXISTS cme_tas_action_events_action_time_idx
    ON public.cme_tas_action_events (action, event_second_utc, product);

CREATE INDEX IF NOT EXISTS cme_tas_action_events_ric_time_idx
    ON public.cme_tas_action_events (ric, event_second_utc);

CREATE TABLE IF NOT EXISTS public.cme_tas_action_load_metadata (
    singleton_id smallint PRIMARY KEY DEFAULT 1 CHECK (singleton_id = 1),
    source_rocksdb text NOT NULL,
    products text[] NOT NULL,
    available_years smallint[] NOT NULL,
    periods text[] NOT NULL,
    start_year smallint NOT NULL,
    end_year smallint NOT NULL,
    partition_count integer NOT NULL,
    raw_status_rows bigint NOT NULL,
    mapped_status_rows bigint NOT NULL,
    staged_action_rows bigint NOT NULL,
    stored_event_rows bigint NOT NULL,
    mapping_version text NOT NULL,
    loaded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (start_year <= end_year),
    CHECK (partition_count > 0),
    CHECK (raw_status_rows >= mapped_status_rows),
    CHECK (staged_action_rows >= stored_event_rows)
);

CREATE OR REPLACE VIEW public.cme_tas_action_consensus AS
SELECT
    product,
    action,
    event_second_utc,
    min(event_ts_utc) AS first_event_ts_utc,
    max(event_ts_utc) AS last_event_ts_utc,
    count(*)::bigint AS ric_count,
    sum(source_event_count)::bigint AS source_event_count,
    bool_and(is_inferred) AS is_inferred,
    min(mapping_version) AS mapping_version
FROM public.cme_tas_action_events
GROUP BY product, action, event_second_utc;

CREATE OR REPLACE VIEW public.cme_tas_action_alignment AS
WITH RECURSIVE direct_consensus AS (
    SELECT
        product,
        action,
        event_second_utc,
        min(event_ts_utc) AS first_event_ts_utc,
        max(event_ts_utc) AS last_event_ts_utc,
        count(*)::bigint AS ric_count,
        sum(source_event_count)::bigint AS source_event_count,
        min(mapping_version) AS mapping_version
    FROM public.cme_tas_action_events
    GROUP BY product, action, event_second_utc
),
open_seconds AS (
    SELECT
        product,
        event_second_utc,
        min(first_event_ts_utc) AS first_event_ts_utc,
        max(last_event_ts_utc) AS last_event_ts_utc,
        sum(ric_count)::bigint AS ric_count,
        sum(source_event_count)::bigint AS source_event_count,
        bool_or(action = 'NEW_PRICE_INDICATION') AS has_new_price_indication,
        bool_or(action = 'TRADING') AS has_trading,
        min(mapping_version) AS mapping_version
    FROM direct_consensus
    WHERE action IN ('NEW_PRICE_INDICATION', 'TRADING')
    GROUP BY product, event_second_utc
),
ordered_open_seconds AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY product ORDER BY event_second_utc
        ) AS row_number
    FROM open_seconds
),
clustered_open_seconds AS (
    SELECT
        *,
        1::bigint AS cluster_id,
        event_second_utc AS cluster_start_utc
    FROM ordered_open_seconds
    WHERE row_number = 1
    UNION ALL
    SELECT
        next.*,
        CASE
            WHEN next.event_second_utc
                <= previous.cluster_start_utc + interval '5 seconds'
            THEN previous.cluster_id
            ELSE previous.cluster_id + 1
        END AS cluster_id,
        CASE
            WHEN next.event_second_utc
                <= previous.cluster_start_utc + interval '5 seconds'
            THEN previous.cluster_start_utc
            ELSE next.event_second_utc
        END AS cluster_start_utc
    FROM clustered_open_seconds AS previous
    JOIN ordered_open_seconds AS next
      ON next.product = previous.product
     AND next.row_number = previous.row_number + 1
),
open_clusters AS (
    SELECT
        product,
        cluster_id,
        min(event_second_utc) AS event_second_utc,
        min(first_event_ts_utc) AS first_event_ts_utc,
        max(last_event_ts_utc) AS last_event_ts_utc,
        sum(ric_count)::bigint AS ric_count,
        sum(source_event_count)::bigint AS source_event_count,
        bool_or(has_new_price_indication) AS has_new_price_indication,
        bool_or(has_trading) AS has_trading,
        min(mapping_version) AS mapping_version,
        extract(epoch FROM max(event_second_utc) - min(event_second_utc))::integer
            AS cluster_span_seconds
    FROM clustered_open_seconds
    GROUP BY product, cluster_id
)
SELECT
    product,
    action,
    event_second_utc,
    first_event_ts_utc,
    last_event_ts_utc,
    ric_count,
    source_event_count,
    false AS is_inferred,
    true AS has_direct_action,
    mapping_version,
    0 AS cluster_span_seconds
FROM direct_consensus
WHERE action IN ('CLOSE', 'PRE_OPEN')
UNION ALL
SELECT
    cluster.product,
    aligned.action,
    cluster.event_second_utc,
    cluster.first_event_ts_utc,
    cluster.last_event_ts_utc,
    cluster.ric_count,
    cluster.source_event_count,
    CASE aligned.action
        WHEN 'NEW_PRICE_INDICATION' THEN NOT cluster.has_new_price_indication
        WHEN 'TRADING' THEN NOT cluster.has_trading
    END AS is_inferred,
    CASE aligned.action
        WHEN 'NEW_PRICE_INDICATION' THEN cluster.has_new_price_indication
        WHEN 'TRADING' THEN cluster.has_trading
    END AS has_direct_action,
    cluster.mapping_version,
    cluster.cluster_span_seconds
FROM open_clusters AS cluster
CROSS JOIN (
    VALUES ('NEW_PRICE_INDICATION'::text), ('TRADING'::text)
) AS aligned(action);

CREATE OR REPLACE VIEW public.cme_tas_action_coverage AS
SELECT
    product,
    action,
    min(event_second_utc) AS first_event_second_utc,
    max(event_second_utc) AS last_event_second_utc,
    count(*)::bigint AS ric_event_count,
    count(DISTINCT ric)::bigint AS distinct_ric_count
FROM public.cme_tas_action_events
GROUP BY product, action;

COMMENT ON TABLE public.cme_tas_action_events IS
    'Per-RIC CME TAS market-phase observations; audit data, not the authoritative exchange calendar.';
COMMENT ON COLUMN public.cme_tas_action_events.is_inferred IS
    'Direct-event table currently stores false; inference is added only by cme_tas_action_alignment.';
COMMENT ON VIEW public.cme_tas_action_consensus IS
    'Product/action/second aggregation over all matching RIC observations.';
COMMENT ON VIEW public.cme_tas_action_alignment IS
    'Comparable phase boundaries; clusters direct CME 15/17 open states within five seconds and explicitly marks a missing counterpart inferred.';

COMMIT;
