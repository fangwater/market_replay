//! Parser parity with the Python implementation: the `*.expected.json`
//! files were produced by `reference/cn_futures_ref` (`parse_html`,
//! `_record_hash`, `parse_payload`, `_session_hash`) on the same inputs.
use cn_reference::json::J;
use cn_reference::models::DB_COLUMNS;
use cn_reference::{services, sessions, source, storage};
use serde_json::Value;
use std::path::PathBuf;

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

#[test]
fn fee_page_matches_python_records_and_hashes() {
    let html = std::fs::read(fixture("fees_small.html")).unwrap();
    let expected: Value =
        serde_json::from_slice(&std::fs::read(fixture("fees_small.expected.json")).unwrap())
            .unwrap();
    let text = source::decode(&html, "text/html").unwrap();
    let records = source::parse_html(&text).unwrap();
    let want = expected["records"].as_array().unwrap();
    assert_eq!(records.len(), want.len());
    assert_eq!(records.len() as u64, expected["count"].as_u64().unwrap());
    for (r, w) in records.iter().zip(want) {
        assert_eq!(r.get("instrument_id"), w["id"].as_str());
        assert_eq!(
            storage::record_hash(r),
            w["hash"].as_str().unwrap(),
            "{:?}",
            w["id"]
        );
        for (i, col) in DB_COLUMNS.iter().enumerate() {
            assert_eq!(r.get(col), w["cols"][i].as_str(), "{} {col}", w["id"]);
        }
    }
    let date = source::source_date(&text, &records, None).unwrap();
    assert_eq!(date, expected["source_date"].as_str().unwrap());
    // No `source_updated_at` in the page: observed_at falls back to midnight UTC.
    let observed = services::observed_at(&records, &date).unwrap();
    assert_eq!(observed.to_rfc3339(), "2026-11-17T00:00:00+00:00");
}

#[test]
fn session_payload_matches_python_rows_and_hashes() {
    let raw = std::fs::read(fixture("sessions_small.json")).unwrap();
    let expected: Value =
        serde_json::from_slice(&std::fs::read(fixture("sessions_small.expected.json")).unwrap())
            .unwrap();
    let rows = sessions::parse_payload(&raw, "application/json").unwrap();
    let want = expected.as_array().unwrap();
    assert_eq!(rows.len(), want.len());
    for (r, w) in rows.iter().zip(want) {
        let key = &w["key"];
        assert_eq!(r.venue, key[0].as_str().unwrap());
        assert_eq!(r.product_id, key[1].as_str().unwrap());
        assert_eq!(r.segment_no, key[2].as_i64().unwrap());
        assert_eq!(r.time_begin, w["time_begin"].as_str().unwrap());
        assert_eq!(r.time_end, w["time_end"].as_str().unwrap());
        assert_eq!(r.crosses_midnight, w["crosses_midnight"].as_bool().unwrap());
        assert_eq!(r.session_group, w["session_group"].as_str().unwrap());
        assert_eq!(r.raw_json, J::s(w["raw_json"].as_str().unwrap()));
        assert_eq!(
            storage::session_hash(r),
            w["hash"].as_str().unwrap(),
            "{key}"
        );
    }
}
