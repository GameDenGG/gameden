import argparse
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from database.migration_guard import get_model_drift_diffs


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Check model/schema drift by comparing SQLAlchemy metadata against the current database schema. "
            "Use this in CI to catch model changes without Alembic migrations."
        )
    )
    parser.add_argument(
        "--fail-on-drift",
        action="store_true",
        help="Exit with non-zero status when drift is detected.",
    )
    args = parser.parse_args()

    diffs = get_model_drift_diffs()
    if not diffs:
        print("Migration drift check: clean")
        return 0

    print(f"Migration drift check: WARNING (diff_count={len(diffs)})")
    for diff in diffs[:20]:
        print(f"- {diff}")
    if len(diffs) > 20:
        print(f"... {len(diffs) - 20} additional diffs omitted")

    if args.fail_on_drift:
        print("Failing because --fail-on-drift is enabled.")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
