from database.models import Session, Game, GamePrice

session = Session()

games = session.query(Game).all()
game_map = {g.name: g.id for g in games}

rows = session.query(GamePrice).filter(GamePrice.game_id.is_(None)).all()

updated = 0
for row in rows:
    game_id = game_map.get(row.game_name)
    if game_id:
        row.game_id = game_id
        updated += 1

session.commit()
session.close()

print(f"Backfilled {updated} rows")
