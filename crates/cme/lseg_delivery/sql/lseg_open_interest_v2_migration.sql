-- One-off upgrade of lseg_open_interest from schema version 1 to 2 (2026-11-26).
-- Version 1 repeated the source path (about 166 bytes) on every data row; version 2 references the
-- source by an integer id. Rows are copied unchanged, ordered by (ric, first_published_ns), and the
-- counts are checked before the old tables are dropped. One transaction: all or nothing.
BEGIN;

DO $$
BEGIN
    IF (SELECT value FROM lseg_open_interest.schema_metadata WHERE key = 'schema_version') <> '1' THEN
        RAISE EXCEPTION 'lseg_open_interest is not at schema version 1';
    END IF;
END $$;

ALTER TABLE lseg_open_interest.sources
    ADD COLUMN source_id integer GENERATED ALWAYS AS IDENTITY CONSTRAINT sources_source_id_key UNIQUE;

DROP VIEW lseg_open_interest.by_trade_date;

CREATE TABLE lseg_open_interest.observations_v2 (
    ric text NOT NULL CHECK (ric <> ''),
    oi_date date,
    open_interest bigint NOT NULL CHECK (open_interest >= 0),
    row_type text NOT NULL,
    first_published_ns bigint NOT NULL,
    last_published_ns bigint NOT NULL,
    publication_rows integer NOT NULL CHECK (publication_rows > 0),
    source_id integer NOT NULL REFERENCES lseg_open_interest.sources (source_id) ON DELETE CASCADE,
    CHECK (last_published_ns >= first_published_ns)
);
INSERT INTO lseg_open_interest.observations_v2
SELECT o.ric, o.oi_date, o.open_interest, o.row_type, o.first_published_ns, o.last_published_ns,
       o.publication_rows, s.source_id
FROM lseg_open_interest.observations o
JOIN lseg_open_interest.sources s USING (source_path)
ORDER BY o.ric, o.first_published_ns;

CREATE TABLE lseg_open_interest.rejects_v2 (
    ric text NOT NULL,
    published_ns bigint,
    raw_value text NOT NULL,
    raw_date text NOT NULL,
    reason text NOT NULL,
    source_id integer NOT NULL REFERENCES lseg_open_interest.sources (source_id) ON DELETE CASCADE
);
INSERT INTO lseg_open_interest.rejects_v2
SELECT r.ric, r.published_ns, r.raw_value, r.raw_date, r.reason, s.source_id
FROM lseg_open_interest.rejects r
JOIN lseg_open_interest.sources s USING (source_path);

DO $$
DECLARE
    old_obs bigint; new_obs bigint; old_rej bigint; new_rej bigint; recorded bigint;
BEGIN
    SELECT count(*) INTO old_obs FROM lseg_open_interest.observations;
    SELECT count(*) INTO new_obs FROM lseg_open_interest.observations_v2;
    SELECT count(*) INTO old_rej FROM lseg_open_interest.rejects;
    SELECT count(*) INTO new_rej FROM lseg_open_interest.rejects_v2;
    SELECT sum(observation_rows) INTO recorded FROM lseg_open_interest.sources;
    IF old_obs <> new_obs OR old_rej <> new_rej OR new_obs <> recorded THEN
        RAISE EXCEPTION 'row counts differ: observations % -> % (sources say %), rejects % -> %',
            old_obs, new_obs, recorded, old_rej, new_rej;
    END IF;
END $$;

DROP TABLE lseg_open_interest.observations;
DROP TABLE lseg_open_interest.rejects;
ALTER TABLE lseg_open_interest.observations_v2 RENAME TO observations;
ALTER TABLE lseg_open_interest.rejects_v2 RENAME TO rejects;
CREATE INDEX observations_ric_date_idx
    ON lseg_open_interest.observations (ric, oi_date, first_published_ns);
CREATE INDEX observations_ric_published_idx
    ON lseg_open_interest.observations (ric, first_published_ns);
CREATE INDEX observations_source_idx ON lseg_open_interest.observations (source_id);
CREATE INDEX rejects_source_idx ON lseg_open_interest.rejects (source_id);

UPDATE lseg_open_interest.schema_metadata SET value = '2' WHERE key = 'schema_version';
COMMIT;
-- The view is recreated by applying lseg_open_interest.sql (tas_open_interest init-schema).
