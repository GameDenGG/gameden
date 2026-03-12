from __future__ import annotations

import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def run_step(command: list[str]) -> int:
    print(f"[frontend-checks] running: {' '.join(command)}")
    completed = subprocess.run(command, cwd=ROOT)
    return int(completed.returncode)


def main() -> int:
    steps = [
        [sys.executable, "scripts/lint_frontend.py"],
        [sys.executable, "-m", "unittest", "tests.test_frontend_dashboard"],
    ]

    for step in steps:
        rc = run_step(step)
        if rc != 0:
            return rc

    print("[frontend-checks] all checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
