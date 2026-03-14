from time import sleep
import time

from config import (
    INGESTION_GAMES_PER_RUN,
    INGESTION_GAMES_PER_RUN_LIMIT,
    INGESTION_LOOP_INTERVAL_SECONDS,
    INGESTION_MAX_DELAY_SECONDS,
    INGESTION_MIN_DELAY_SECONDS,
    INGESTION_SHARD_INDEX,
    INGESTION_SHARD_TOTAL,
    validate_settings,
)
from database import direct_engine
from database.schema_guard import assert_scale_schema_ready

from jobs.ingest_prices import run_price_ingestion

INTERVAL_SECONDS = INGESTION_LOOP_INTERVAL_SECONDS


def run_loop() -> None:
    validate_settings()
    assert_scale_schema_ready(direct_engine, component_name="price ingestion scheduler")
    print(
        "price ingestion scheduler started "
        f"interval_seconds={INTERVAL_SECONDS} "
        f"track_games_per_run={INGESTION_GAMES_PER_RUN} "
        f"track_games_per_run_limit={INGESTION_GAMES_PER_RUN_LIMIT} "
        f"delay_range_seconds={INGESTION_MIN_DELAY_SECONDS:.2f}-{INGESTION_MAX_DELAY_SECONDS:.2f} "
        f"shard={INGESTION_SHARD_INDEX}/{INGESTION_SHARD_TOTAL}"
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
