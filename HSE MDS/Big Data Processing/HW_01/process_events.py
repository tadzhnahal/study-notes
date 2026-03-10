import argparse
import functools
import json
import logging
import time
from collections.abc import Iterable, Iterator
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

WORKDIR = Path(__file__).parent

logging.basicConfig(level=logging.INFO, format="%(processName)s %(message)s")
logger = logging.getLogger(__name__)


def slowlog(threshold: float = 2.5):
    def set_timer(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start = time.perf_counter()
            try:
                result = func(*args, **kwargs)
            finally:
                delta = time.perf_counter() - start
                if delta > threshold:
                    logger.info("Finished %s in %.3f seconds (too slow)", func.__name__, delta)
                else:
                    logger.info("Finished %s in %.3f seconds", func.__name__, delta)
            return result
        return wrapper
    return set_timer


def read_events(path: Path) -> Iterator[dict]:
    with open(path, "r", encoding="utf-8") as f:
        for line_number, line in enumerate(f, start=1):
            try:
                event = json.loads(line)
                yield event
            except json.JSONDecodeError:
                logger.warning("Failed to parse line %d", line_number)


def filter_events(events: Iterator[dict], wanted_event_types) -> Iterator[dict]:
    if isinstance(wanted_event_types, str):
        wanted_event_types = [wanted_event_types]
    else:
        wanted_event_types = list(wanted_event_types)

    for event in events:
        if event["event_type"] in wanted_event_types:
            yield event


def batcher(iterable: Iterable[dict], batch_size: int) -> Iterable[list[dict]]:
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) == batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


@slowlog(5)
def process_batch(batch: list[dict], strategy: str = "currency") -> dict:
    time.sleep(3)

    count = 0
    revenue = {}

    for event in batch:
        count += 1

        # берём price, если нет - смотрим amount, если и его нет - 0
        price = event.get("price")
        if price is None:
            price = event.get("amount", 0)

        if strategy == "currency":
            key = event.get("currency")
        elif strategy == "source":
            key = event.get("source")
        elif strategy == "month":
            ts = event.get("ts")
            if ts:
                key = ts[:7]  # берём YYYY-MM
            else:
                key = None
        else:
            key = None

        if key is not None:
            if key in revenue:
                revenue[key] += price
            else:
                revenue[key] = price

    return {"count": count, "revenue": revenue}


def process_batches(batches: Iterable[list[dict]], strategy: str = "currency", max_workers: int = 8) -> dict:
    total_count = 0
    total_revenue = {}

    with ProcessPoolExecutor() as executor:
        chunk = []

        for batch in batches:
            chunk.append(batch)

            if len(chunk) == max_workers:
                futures = [executor.submit(process_batch, b, strategy) for b in chunk]
                for future in as_completed(futures):
                    result = future.result()
                    total_count += result["count"]
                    for k, amount in result["revenue"].items():
                        if k in total_revenue:
                            total_revenue[k] += amount
                        else:
                            total_revenue[k] = amount
                chunk = []

        if chunk:
            futures = [executor.submit(process_batch, b, strategy) for b in chunk]
            for future in as_completed(futures):
                result = future.result()
                total_count += result["count"]
                for k, amount in result["revenue"].items():
                    if k in total_revenue:
                        total_revenue[k] += amount
                    else:
                        total_revenue[k] = amount

    return {"total_count": total_count, "total_revenue": total_revenue}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Process events")
    parser.add_argument("--strategy", choices=["currency", "source", "month"], default="currency")
    parser.add_argument("--batch-size", type=int, default=50_000)
    parser.add_argument("--event-type", nargs="+", default=["purchase"])
    return parser.parse_args()


@slowlog(20)
def main():
    args = parse_args()

    filename = WORKDIR / "data" / "events.jsonl"

    # читаем, фильтруем, делим на батчи
    events = read_events(filename)
    filtered_events = filter_events(events, args.event_type)
    batches = batcher(filtered_events, batch_size=args.batch_size)

    result = process_batches(batches, strategy=args.strategy)

    logger.info("Total events: %d", result["total_count"])
    logger.info("Total revenue (%s): %s", args.strategy, result["total_revenue"])


if __name__ == "__main__":
    main()