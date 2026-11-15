BEGIN;

CREATE OR REPLACE VIEW public.cme_six_product_trading_sessions AS
WITH product_map(product, exchange, schedule_group) AS (
    VALUES
        ('ES'::text, 'CME'::text, 'equity_indices'::text),
        ('NQ'::text, 'CME'::text, 'equity_indices'::text),
        ('RTY'::text, 'CME'::text, 'equity_indices'::text),
        ('YM'::text, 'CBOT'::text, 'equity_indices'::text),
        ('GC'::text, 'COMEX'::text, 'metals'::text),
        ('CL'::text, 'NYMEX'::text, 'energy'::text)
),
snapshot_days AS (
    SELECT
        snapshot_id,
        schedule_group,
        utc_date,
        max(loaded_at) AS snapshot_loaded_at
    FROM public.cme_globex_daily_trading_intervals
    WHERE schedule_group IN ('equity_indices', 'metals', 'energy')
    GROUP BY snapshot_id, schedule_group, utc_date
),
ranked_snapshot_days AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY schedule_group, utc_date
            ORDER BY snapshot_loaded_at DESC, snapshot_id DESC
        ) AS snapshot_rank
    FROM snapshot_days
),
selected_intervals AS (
    SELECT interval.*
    FROM public.cme_globex_daily_trading_intervals AS interval
    JOIN ranked_snapshot_days AS selected
      ON selected.snapshot_id = interval.snapshot_id
     AND selected.schedule_group = interval.schedule_group
     AND selected.utc_date = interval.utc_date
     AND selected.snapshot_rank = 1
    WHERE interval.is_trading
),
product_intervals AS (
    SELECT
        product.product,
        product.exchange,
        interval.schedule_group,
        (
            (interval.open_utc AT TIME ZONE 'America/Chicago')::date
            + CASE
                WHEN (interval.open_utc AT TIME ZONE 'America/Chicago')::time
                    >= time '17:00'
                THEN 1
                ELSE 0
              END
        )::date AS trade_date,
        interval.open_utc,
        interval.close_utc,
        interval.schedule_regime,
        interval.exception_name,
        interval.confidence,
        interval.snapshot_id,
        interval.source_url
    FROM selected_intervals AS interval
    JOIN product_map AS product USING (schedule_group)
),
marked_intervals AS (
    SELECT
        *,
        CASE
            WHEN lag(close_utc) OVER (
                PARTITION BY product, trade_date
                ORDER BY open_utc, close_utc
            ) = open_utc
            THEN 0
            ELSE 1
        END AS starts_segment
    FROM product_intervals
),
grouped_intervals AS (
    SELECT
        *,
        sum(starts_segment) OVER (
            PARTITION BY product, trade_date
            ORDER BY open_utc, close_utc
            ROWS UNBOUNDED PRECEDING
        ) AS segment_group
    FROM marked_intervals
),
merged_segments AS (
    SELECT
        product,
        exchange,
        schedule_group,
        trade_date,
        segment_group,
        min(open_utc) AS open_utc,
        max(close_utc) AS close_utc,
        extract(epoch FROM max(close_utc) - min(open_utc))::integer
            AS duration_seconds,
        count(*)::integer AS source_fragment_count,
        array_agg(DISTINCT schedule_regime ORDER BY schedule_regime)
            AS schedule_regimes,
        coalesce(
            array_agg(DISTINCT exception_name ORDER BY exception_name)
                FILTER (WHERE exception_name <> ''),
            ARRAY[]::text[]
        ) AS exception_names,
        CASE min(
            CASE confidence
                WHEN 'high' THEN 3
                WHEN 'medium' THEN 2
                WHEN 'low' THEN 1
                ELSE 0
            END
        )
            WHEN 3 THEN 'high'
            WHEN 2 THEN 'medium'
            WHEN 1 THEN 'low'
            ELSE 'not_applicable'
        END AS confidence,
        array_agg(DISTINCT snapshot_id ORDER BY snapshot_id) AS snapshot_ids,
        array_agg(DISTINCT source_url ORDER BY source_url) AS source_urls
    FROM grouped_intervals
    GROUP BY product, exchange, schedule_group, trade_date, segment_group
),
numbered_segments AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY product, trade_date
            ORDER BY open_utc, close_utc
        )::smallint AS segment_index
    FROM merged_segments
)
SELECT
    product,
    exchange,
    schedule_group,
    trade_date,
    segment_index,
    open_utc,
    close_utc,
    duration_seconds,
    source_fragment_count,
    schedule_regimes,
    exception_names,
    confidence,
    snapshot_ids,
    source_urls
FROM numbered_segments;

CREATE OR REPLACE VIEW public.cme_six_product_session_validation AS
WITH tas_coverage AS (
    SELECT
        product,
        min(event_second_utc) AS first_tas_event_utc,
        max(event_second_utc) AS last_tas_event_utc
    FROM public.cme_tas_action_events
    GROUP BY product
)
SELECT
    session.*,
    open_evidence.event_second_utc AS tas_open_utc,
    CASE
        WHEN open_evidence.event_second_utc IS NULL THEN NULL
        ELSE extract(epoch FROM open_evidence.event_second_utc - session.open_utc)
    END AS tas_open_delta_seconds,
    CASE
        WHEN open_evidence.event_second_utc IS NULL THEN NULL
        ELSE NOT open_evidence.has_direct_trading
    END AS tas_open_is_inferred,
    open_evidence.ric_count AS tas_open_ric_count,
    close_evidence.event_second_utc AS tas_close_utc,
    CASE
        WHEN close_evidence.event_second_utc IS NULL THEN NULL
        ELSE extract(epoch FROM close_evidence.event_second_utc - session.close_utc)
    END AS tas_close_delta_seconds,
    close_evidence.ric_count AS tas_close_ric_count,
    coverage.first_tas_event_utc,
    coverage.last_tas_event_utc,
    CASE
        WHEN coverage.product IS NULL
          OR session.close_utc < coverage.first_tas_event_utc
          OR session.open_utc > coverage.last_tas_event_utc
        THEN 'OUTSIDE_TAS_COVERAGE'
        WHEN open_evidence.event_second_utc IS NOT NULL
         AND close_evidence.event_second_utc IS NOT NULL
        THEN 'MATCHED'
        WHEN open_evidence.event_second_utc IS NOT NULL
          OR close_evidence.event_second_utc IS NOT NULL
        THEN 'PARTIAL'
        ELSE 'NO_TAS_EVIDENCE'
    END AS tas_validation
FROM public.cme_six_product_trading_sessions AS session
LEFT JOIN tas_coverage AS coverage USING (product)
LEFT JOIN LATERAL (
    SELECT
        event_second_utc,
        count(DISTINCT ric)::bigint AS ric_count,
        bool_or(action.action = 'TRADING') AS has_direct_trading
    FROM public.cme_tas_action_events AS action
    WHERE action.product = session.product
      AND action.action IN ('NEW_PRICE_INDICATION', 'TRADING')
      AND action.event_second_utc BETWEEN
          session.open_utc - interval '2 seconds'
          AND session.open_utc + interval '2 seconds'
    GROUP BY event_second_utc
    ORDER BY
        abs(extract(epoch FROM action.event_second_utc - session.open_utc)),
        action.event_second_utc
    LIMIT 1
) AS open_evidence ON true
LEFT JOIN LATERAL (
    SELECT
        event_second_utc,
        count(*)::bigint AS ric_count
    FROM public.cme_tas_action_events AS action
    WHERE action.product = session.product
      AND action.action = 'CLOSE'
      AND action.event_second_utc BETWEEN
          session.close_utc - interval '2 seconds'
          AND session.close_utc + interval '2 seconds'
    GROUP BY event_second_utc
    ORDER BY
        abs(extract(epoch FROM action.event_second_utc - session.close_utc)),
        action.event_second_utc
    LIMIT 1
) AS close_evidence ON true;

COMMENT ON VIEW public.cme_six_product_trading_sessions IS
    'Query-ready official CME sessions for ES/NQ/RTY/YM/GC/CL. UTC fragments are merged by Chicago trade date; a real outage remains a separate segment.';
COMMENT ON VIEW public.cme_six_product_session_validation IS
    'Official six-product sessions with nearby TAS phase observations. Official open/close remain authoritative; TAS Date-Time is validation evidence only.';

COMMIT;
