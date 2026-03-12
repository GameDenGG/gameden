import sys
import time
from pathlib import Path
from datetime import datetime, timezone

sys.path.append(str(Path(__file__).resolve().parents[1]))

import schedule

from config import validate_settings, PRICE_CHECK_INTERVAL_MINUTES
from main import track_all_games
from logger_config import setup_logger

logger = setup_logger("scheduler")

validate_settings()

_is_job_running = False
_last_started_at = None
_last_finished_at = None
_last_status = "never_ran"
_run_count = 0


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def fmt_dt(value: datetime | None) -> str:
    if value is None:
        return "—"
    return value.isoformat()


def log_scheduler_status() -> None:
    jobs = schedule.get_jobs()
    next_run = jobs[0].next_run.isoformat() if jobs and jobs[0].next_run else "—"

    logger.info(
        (
            "Scheduler status | runs=%s running=%s last_status=%s "
            "last_started_at=%s last_finished_at=%s next_run=%s"
        ),
        _run_count,
        _is_job_running,
        _last_status,
        fmt_dt(_last_started_at),
        fmt_dt(_last_finished_at),
        next_run,
    )


def job() -> None:
    global _is_job_running
    global _last_started_at
    global _last_finished_at
    global _last_status
    global _run_count

    if _is_job_running:
        logger.warning("Previous scheduled job is still running. Skipping this cycle.")
        log_scheduler_status()
        return

    _is_job_running = True
    _run_count += 1
    _last_started_at = utc_now()
    _last_status = "running"

    logger.info("Scheduled job started. run_number=%s started_at=%s", _run_count, fmt_dt(_last_started_at))

    try:
        track_all_games()
        _last_status = "success"
        logger.info("Scheduled job finished successfully.")
    except Exception:
        _last_status = "failed"
        logger.exception("Scheduled job failed.")
    finally:
        _last_finished_at = utc_now()
        duration_seconds = round((_last_finished_at - _last_started_at).total_seconds(), 2)
        _is_job_running = False

        logger.info(
            "Scheduled job finished. status=%s duration_seconds=%s finished_at=%s",
            _last_status,
            duration_seconds,
            fmt_dt(_last_finished_at),
        )
        log_scheduler_status()


def main() -> None:
    interval_minutes = PRICE_CHECK_INTERVAL_MINUTES

    if interval_minutes <= 0:
        raise ValueError(
            f"PRICE_CHECK_INTERVAL_MINUTES must be greater than 0, got {interval_minutes}"
        )

    schedule.every(interval_minutes).minutes.do(job)

    logger.info(
        "Scheduler started. Running every %s minutes.",
        interval_minutes,
    )
    log_scheduler_status()

    logger.info("Running initial job on startup.")
    job()

    last_heartbeat_minute = None

    try:
        while True:
            schedule.run_pending()

            now = utc_now()
            if now.minute != last_heartbeat_minute and now.second < 5:
                last_heartbeat_minute = now.minute
                log_scheduler_status()

            time.sleep(5)
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user.")
        log_scheduler_status()
    except Exception:
        logger.exception("Scheduler crashed unexpectedly.")
        log_scheduler_status()
        raise


if __name__ == "__main__":
    main()