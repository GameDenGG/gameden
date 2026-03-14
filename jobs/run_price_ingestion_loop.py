import os
from time import sleep
import time

from database import direct_engine
from database.schema_guard import assert_scale_schema_ready

# Backward-compatible alias: allow operators to set ingestion batch size
# without changing TRACK_GAMES_PER_RUN directly.
if os.getenv("INGESTION_BATCH_SIZE") and not os.getenv("TRACK_GAMES_PER_RUN"):
    os.environ["TRACK_GAMES_PER_RUN"] = str(os.getenv("INGESTION_BATCH_SIZE"))
if os.getenv("INGESTION_BATCH_SIZE_LIMIT") and not os.getenv("TRACK_GAMES_PER_RUN_LIMIT"):
    os.environ["TRACK_GAMES_PER_RUN_LIMIT"] = str(os.getenv("INGESTION_BATCH_SIZE_LIMIT"))

from jobs.ingest_prices import run_price_ingestion

INTERVAL_SECONDS = max(5, int(os.getenv("INGESTION_LOOP_INTERVAL_SECONDS", "300")))


def run_loop() -> None:
    assert_scale_schema_ready(direct_engine, component_name="price ingestion scheduler")
    print(
        "price ingestion scheduler started "
        f"interval_seconds={INTERVAL_SECONDS} "
        f"track_games_per_run={os.getenv('TRACK_GAMES_PER_RUN', 'default')} "
        f"track_games_per_run_limit={os.getenv('TRACK_GAMES_PER_RUN_LIMIT', 'default')} "
        f"shard={os.getenv('TRACK_SHARD_INDEX', '0')}/{os.getenv('TRACK_SHARD_TOTAL', '1')}"
    )

    while True:
        started = time.perf_counter()
        try:
            print("running price ingestion cycle...")
            run_price_ingestion()
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            print(f"price ingestion complete elapsed_ms={elapsed_ms}")
        except Exception as e:
            print(f"price ingestion error: {e}")
            elapsed_ms = int((time.perf_counter() - started) * 1000)

        remaining_sleep = max(0.0, INTERVAL_SECONDS - (elapsed_ms / 1000.0))
        print(f"price ingestion scheduler sleeping {remaining_sleep:.2f}s")
        sleep(remaining_sleep)


if __name__ == "__main__":
    run_loop()
