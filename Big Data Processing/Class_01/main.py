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
        for line in f:
            yield json.loads(line)


def filter_events(events: Iterator[dict],  wanted_event_type: str) -> Iterator[dict]:
    for event in events:
        if event["event_type"] == wanted_event_type:
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
def process_batch(batch: list[dict]) -> int:
    time.sleep(3)  # CPU operation
    return len(batch)


@slowlog(20)
def main():
    filename = WORKDIR / "data" / "events.jsonl"
    events = read_events(filename)
    filtered_events = filter_events(events, "purchase")
    batches = batcher(filtered_events, batch_size=50_000)

    total = 0
    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(process_batch, batch) for batch in batches]
        for future in as_completed(futures):
            total += future.result()
    logger.info("Total events: %d", total)


if __name__ == "__main__":
    main()
