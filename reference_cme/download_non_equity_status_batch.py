import hashlib
import json
import time
from pathlib import Path

import databento as db
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parent
STATE_PATH = PROJECT_ROOT / ".non_equity_status_batch.json"
OUTPUT_DIR = PROJECT_ROOT / "batch_downloads"

DATASET = "GLBX.MDP3"
SCHEMA = "status"
START = "2020-01-01"
SYMBOLS = [
    "GC.FUT",
    "CL.FUT",
    "SR3.FUT",  # LSEG SRA
    "KRW.FUT",
    "6E.FUT",  # LSEG URO
    "SR1.FUT",  # LSEG S1R
]


def save_state(state: dict[str, object]) -> None:
    temp_path = STATE_PATH.with_suffix(".tmp")
    temp_path.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")
    temp_path.replace(STATE_PATH)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


load_dotenv(PROJECT_ROOT / ".env")
client = db.Historical()

if STATE_PATH.exists():
    state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    job_id = str(state["job_id"])
    print(f"resuming batch job: {job_id}", flush=True)
else:
    end = client.metadata.get_dataset_range(dataset=DATASET)["schema"][SCHEMA]["end"]
    estimated_cost = client.metadata.get_cost(
        dataset=DATASET,
        symbols=SYMBOLS,
        stype_in="parent",
        schema=SCHEMA,
        start=START,
        end=end,
    )
    print(f"end: {end}", flush=True)
    print(f"estimated cost: ${estimated_cost:.6f}", flush=True)

    job = client.batch.submit_job(
        dataset=DATASET,
        symbols=SYMBOLS,
        stype_in="parent",
        schema=SCHEMA,
        start=START,
        end=end,
        encoding="dbn",
        compression="zstd",
        split_symbols=False,
        split_duration="none",
        delivery="download",
    )
    job_id = str(job["id"])
    state = {
        "job_id": job_id,
        "dataset": DATASET,
        "schema": SCHEMA,
        "symbols": SYMBOLS,
        "start": START,
        "end": end,
        "estimated_cost_usd": estimated_cost,
    }
    save_state(state)
    print(f"submitted batch job: {job_id}", flush=True)

downloaded_paths = [Path(path) for path in state.get("downloaded_paths", [])]
if downloaded_paths and all(path.is_file() for path in downloaded_paths):
    print("batch files already downloaded:", flush=True)
else:
    while True:
        details = client.batch.get_job_details(job_id=job_id)
        job_state = str(details["state"])
        progress = details.get("progress")
        print(f"state={job_state} progress={progress}", flush=True)
        if job_state == "done":
            break
        if job_state != "queued" and job_state != "processing":
            raise RuntimeError(f"batch job stopped in unexpected state: {job_state}")
        time.sleep(30)

    downloaded_paths = client.batch.download(job_id=job_id, output_dir=OUTPUT_DIR)
    state["downloaded_paths"] = [str(path) for path in downloaded_paths]
    state["actual_cost_usd"] = details.get("cost_usd")
    state["record_count"] = details.get("record_count")
    save_state(state)

dbn_paths: list[Path] = []
for path in downloaded_paths:
    if path.name.endswith((".dbn", ".dbn.zst")):
        store = db.DBNStore.from_file(path)
        if str(store.metadata.dataset) != DATASET or str(store.metadata.schema) != SCHEMA:
            raise RuntimeError(f"unexpected DBN metadata in {path}")
        dbn_paths.append(path)
        print(
            f"verified: {path} size={path.stat().st_size} sha256={sha256(path)}",
            flush=True,
        )

if not dbn_paths:
    raise RuntimeError("batch download did not contain a DBN file")

print(f"record_count: {state.get('record_count')}", flush=True)
print(f"actual cost: ${float(state.get('actual_cost_usd') or 0):.6f}", flush=True)

