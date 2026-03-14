#!/usr/bin/env bash

export DATABASE_URL='postgresql://gameden:6SiNdmGLaJCTvhTwHlGx8DP4SfFBBhIe@dpg-d6p59enkijhs73fibimg-a.oregon-postgres.render.com/gameden'
export PYTHONUNBUFFERED=1

echo "Starting GameDen pipeline..."

echo "Using database:"
echo $DATABASE_URL
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
echo "Press Ctrl+C to stop everything."

cleanup() {
  kill $INGEST_PID $SNAP_PID 2>/dev/null
  exit
}

trap cleanup INT TERM

while true; do
  echo ""
  echo "---- GameDen Health Stats ----"

python - << 'PYEOF'
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

  sleep 60
done
