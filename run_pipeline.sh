#!/usr/bin/env bash
set -euo pipefail

export PYTHONUNBUFFERED=1

echo "Starting GameDen pipeline..."
echo "DATABASE_URL is set: ${DATABASE_URL:+yes}"
echo ""

echo "Starting ingestion worker..."
python -m jobs.run_price_ingestion_loop &
INGEST_PID=$!

echo "Starting snapshot worker..."
python -m jobs.refresh_snapshots &
SNAP_PID=$!

echo "Workers started:"
echo "Ingestion PID: $INGEST_PID"
echo "Snapshot PID: $SNAP_PID"

cleanup() {
  echo ""
  echo "Shutting down workers..."
  kill "$INGEST_PID" "$SNAP_PID" 2>/dev/null || true
  wait "$INGEST_PID" "$SNAP_PID" 2>/dev/null || true
  exit 0
}

trap cleanup INT TERM

while true; do
  echo ""
  echo "---- GameDen Health Stats ----"

  python - <<'PYEOF'
from sqlalchemy import text
from database.models import Session

s = Session()
try:
    print("games=", s.execute(text("SELECT COUNT(*) FROM games")).scalar())
    print("prices=", s.execute(text("SELECT COUNT(*) FROM game_prices")).scalar())
    print("players=", s.execute(text("SELECT COUNT(*) FROM game_player_history")).scalar())
    print("snapshots=", s.execute(text("SELECT COUNT(*) FROM game_snapshots")).scalar())
    print("dirty_queue=", s.execute(text("SELECT COUNT(*) FROM dirty_games")).scalar())
finally:
    s.close()
PYEOF

  if ! kill -0 "$INGEST_PID" 2>/dev/null; then
    echo "Ingestion worker died. Exiting so Render can restart."
    exit 1
  fi

  if ! kill -0 "$SNAP_PID" 2>/dev/null; then
    echo "Snapshot worker died. Exiting so Render can restart."
    exit 1
  fi

  sleep 60
done