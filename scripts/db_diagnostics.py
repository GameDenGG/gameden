import sys
from pathlib import Path

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from database import direct_engine


def _print_rows(title: str, sql: str) -> None:
    print(f"\n=== {title} ===")
    with direct_engine.connect() as conn:
        rows = conn.execute(text(sql)).fetchall()
    if not rows:
        print("<empty>")
        return

    for idx, row in enumerate(rows, start=1):
        query = " ".join((row.query or "").split())
        if len(query) > 180:
            query = query[:177] + "..."
        print(
            f"{idx:02d}. calls={row.calls} total_ms={row.total_exec_time:.2f} "
            f"mean_ms={row.mean_exec_time:.2f} rows={row.rows} sql={query}"
        )


def main() -> None:
    try:
        with direct_engine.connect() as conn:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_stat_statements"))
            conn.commit()
    except Exception as exc:
        print(f"Failed enabling pg_stat_statements: {exc}")
        return

    slowest_sql = """
        SELECT query, calls, total_exec_time, mean_exec_time, rows
        FROM pg_stat_statements
        ORDER BY total_exec_time DESC
        LIMIT 20
    """

    most_called_sql = """
        SELECT query, calls, total_exec_time, mean_exec_time, rows
        FROM pg_stat_statements
        ORDER BY calls DESC
        LIMIT 20
    """

    try:
        _print_rows("Top 20 Slowest By Total Time", slowest_sql)
        _print_rows("Top 20 Most Called", most_called_sql)
    except Exception as exc:
        print(f"Failed querying pg_stat_statements: {exc}")


if __name__ == "__main__":
    main()
