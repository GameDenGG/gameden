import os
import subprocess
import sys


def _run_migrations() -> None:
    command = [sys.executable, "-m", "alembic", "upgrade", "head"]
    print("Running database migrations: alembic upgrade head")
    subprocess.run(command, check=True)
    print("Database migrations complete.")


def _start_api() -> None:
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))
    command = [sys.executable, "-m", "uvicorn", "api.server:app", "--host", host, "--port", str(port)]
    print(f"Starting API server on {host}:{port}")
    os.execvp(command[0], command)


def main() -> None:
    _run_migrations()
    _start_api()


if __name__ == "__main__":
    main()
