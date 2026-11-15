"""Download Nasdaq SK hynix depth and trades; reruns resume the recorded jobs."""
import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import databento as db
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
STATE = ROOT / "batch_downloads" / "skhynix_202606_present.json"
DATASET = "XNAS.ITCH"
SYMBOLS = ["SKHY", "SKHYV"]
SCHEMAS = ["mbp-10", "trades"]


def save(state):
    STATE.parent.mkdir(parents=True, exist_ok=True)
    temp = STATE.with_suffix(".tmp")
    temp.write_text(json.dumps(state, indent=2) + "\n")
    temp.replace(STATE)


def main():
    load_dotenv(ROOT / ".env")
    client = db.Historical()
    if STATE.exists():
        state = json.loads(STATE.read_text())
    else:
        ranges = client.metadata.get_dataset_range(dataset=DATASET)["schema"]
        end = min(datetime.now(timezone.utc), *[
            datetime.fromisoformat(ranges[s]["end"].replace("Z", "+00:00"))
            for s in SCHEMAS
        ]).isoformat()
        state = dict(dataset=DATASET, symbols=SYMBOLS, start="2026-06-01", end=end, jobs={})
        state["symbology"] = client.symbology.resolve(
            dataset=DATASET, symbols=SYMBOLS, stype_in="raw_symbol",
            stype_out="instrument_id", start_date=state["start"], end_date=end[:10],
        )
        state["estimated_cost_usd"] = {
            s: client.metadata.get_cost(dataset=DATASET, symbols=SYMBOLS,
                schema=s, start=state["start"], end=end) for s in SCHEMAS
        }
        save(state)
    print(json.dumps({k: v for k, v in state.items() if k != "symbology"}, indent=2), flush=True)
    for schema in SCHEMAS:
        if schema not in state["jobs"]:
            job = client.batch.submit_job(
                dataset=DATASET, symbols=SYMBOLS, schema=schema,
                start=state["start"], end=state["end"], stype_in="raw_symbol",
                encoding="dbn", compression="zstd", split_duration="day",
            )
            state["jobs"][schema] = {"job_id": str(job["id"])}
            save(state)
            print(f"submitted {schema}: {job['id']}", flush=True)
    for schema in SCHEMAS:
        job = state["jobs"][schema]
        if job.get("verified_files") and all(
            Path(f["path"]).is_file() and Path(f["path"]).stat().st_size == f["bytes"]
            for f in job["verified_files"]
        ):
            print(f"already downloaded: {schema}", flush=True)
            continue
        while True:
            details = client.batch.get_job_details(job_id=job["job_id"])
            print(f"{schema}: {details['state']} progress={details.get('progress')}", flush=True)
            if details["state"] == "done":
                break
            if details["state"] not in {"queued", "processing", "received"}:
                raise RuntimeError(f"Unexpected job state: {details['state']}")
            time.sleep(15)
        paths = client.batch.download(job_id=job["job_id"], output_dir=ROOT / "batch_downloads")
        verified = []
        for path in paths:
            path = Path(path)
            if not path.name.endswith((".dbn", ".dbn.zst")):
                continue
            metadata = db.DBNStore.from_file(path).metadata
            if str(metadata.dataset) != DATASET or str(metadata.schema) != schema:
                raise RuntimeError(f"Incorrect dataset/schema: {path}")
            with path.open("rb") as handle:
                digest = hashlib.file_digest(handle, "sha256").hexdigest()
            verified.append(dict(path=str(path), bytes=path.stat().st_size, sha256=digest))
        if not verified:
            raise RuntimeError(f"No DBN files in {job['job_id']}")
        job.update(verified_files=verified, actual_cost_usd=details.get("cost_usd"),
                   record_count=details.get("record_count"))
        save(state)
        print(f"verified {schema}: {len(verified)} files, {sum(f['bytes'] for f in verified)} bytes", flush=True)


if __name__ == "__main__":
    main()
