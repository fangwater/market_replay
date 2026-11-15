from datetime import datetime, timezone
from pathlib import Path

import databento as db
from dotenv import load_dotenv


load_dotenv(Path(__file__).with_name(".env"))

client = db.Historical()

dataset = "GLBX.MDP3"
schema = "status"
requested_end = datetime(2026, 9, 4, tzinfo=timezone.utc)

symbols = [
    "ES.FUT",
    "NQ.FUT",
    "YM.FUT",
    "RTY.FUT",
]

dataset_range = client.metadata.get_dataset_range(dataset=dataset)
available_end_text = dataset_range["schema"][schema]["end"]
available_end = datetime.fromisoformat(available_end_text.replace("Z", "+00:00"))
effective_end = min(requested_end, available_end)

if effective_end < requested_end:
    print(f"end adjusted to available data: {available_end_text}")

cost = client.metadata.get_cost(
    dataset=dataset,
    symbols=symbols,
    stype_in="parent",
    schema=schema,
    start="2024-01-01",
    end=effective_end,
)

print(f"estimated cost: ${cost:.6f}")
