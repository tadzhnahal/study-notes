import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

WORKDIR = Path(__file__).parent

EVENT_TYPES = ["view", "click", "purchase", "refund"]
SOURCES = ["web", "mobile", "api"]
CURRENCIES = ["USD", "EUR"]


def random_ts(start: datetime, end: datetime) -> str:
    delta = end - start
    seconds = random.randint(0, int(delta.total_seconds()))
    return (start + timedelta(seconds=seconds)).isoformat()


def generate_event(start: datetime, end: datetime) -> dict:
    event_type = random.choices(
        EVENT_TYPES,
        weights=[0.6, 0.25, 0.13, 0.02],
        k=1,
    )[0]

    price = round(random.uniform(5, 500), 2) if event_type in {"purchase", "refund"} else 0.0

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "user_id": random.randint(1, 50_000),
        "price": price,
        "currency": random.choice(CURRENCIES),
        "ts": random_ts(start, end),
        "source": random.choice(SOURCES),
    }


def main(output_path: Path, events_count: int = 5_000_000):
    start = datetime.now(tz=timezone.utc) - timedelta(days=30)
    end = datetime.now(tz=timezone.utc)

    with output_path.open("w", encoding="utf-8") as f:
        for _ in range(events_count):
            event = generate_event(start, end)
            f.write(json.dumps(event) + "\n")


if __name__ == "__main__":
    output = Path(WORKDIR / "data" / "events.jsonl")
    output.parent.mkdir(parents=True, exist_ok=True)
    main(output)
