from time import sleep
import time

from database import direct_engine
from database.schema_guard import assert_scale_schema_ready
from jobs.ingest_prices import run_price_ingestion

INTERVAL_SECONDS = 300


def run_loop() -> None:
    assert_scale_schema_ready(direct_engine, component_name="price ingestion scheduler")
    print("price ingestion scheduler started")

    while True:
        started = time.perf_counter()
        try:
            print("running price ingestion...")
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
