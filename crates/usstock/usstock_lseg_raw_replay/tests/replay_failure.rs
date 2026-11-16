use std::fs;
use std::path::Path;
use std::process::Command;
use tempfile::TempDir;
use usstock_lseg_raw_replay::quote_replay::raw_building_path;

#[test]
fn worker_error_does_not_publish_or_leave_building_db() {
    let temp = TempDir::new().unwrap();
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/merged-Data-part-000000-shard-000000.csv");
    let mut inputs = Vec::new();
    for shard in 0..15 {
        let path = temp
            .path()
            .join(format!("merged-Data-part-000000-shard-{shard:06}.csv"));
        fs::copy(&fixture, &path).unwrap();
        inputs.push(path);
    }
    let bad = temp.path().join("merged-Data-part-000000-shard-000015.csv");
    fs::write(
        &bad,
        concat!(
            "#RIC,Domain,Date-Time,GMT Offset,Type,MsgClass/FID number,UpdateType/Action,FID Name,FID Value,FID Enum String,PE Code,Template Number,Key/Msg Sequence Number,Number of FIDs\n",
            // Unknown FIDs are kept as auxiliary fields; a duplicated Quote
            // FID is still a hard error and must fail the whole replay.
            "AAPL.O,Market Price,2021-07-01T00:00:00.100000000Z,-4,Raw,UPDATE,QUOTE,,,,74,,1,2\n",
            ",,,,FID,22,,BID,100.08,\n",
            ",,,,FID,22,,BID,100.09,\n",
        ),
    )
    .unwrap();
    inputs.push(bad);

    let output = temp.path().join("failed-output");
    let config = temp.path().join("failure.toml");
    let input_lines = inputs
        .iter()
        .map(|path| format!("  \"{}\",", path.display()))
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(
        &config,
        format!(
            "period = \"failure-fixture\"\ninputs = [\n{input_lines}\n]\nrocksdb_dir = \"{}\"\nprogress_every = 0\nkeep_temporary_column_families = false\nworkers = 16\n",
            output.display()
        ),
    )
    .unwrap();

    let result = Command::new(env!("CARGO_BIN_EXE_usstock_lseg_raw_rocksdb"))
        .arg("--config")
        .arg(&config)
        .env("RUST_BACKTRACE", "0")
        .output()
        .unwrap();
    assert!(
        !result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(stderr.contains("duplicate Quote FID"), "{stderr}");
    assert!(stderr.contains("incomplete output was removed"), "{stderr}");
    assert!(!output.exists());
    assert!(!raw_building_path(&output).exists());
}
