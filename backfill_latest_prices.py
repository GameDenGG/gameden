from database.models import Session, Game, GamePrice, GameLatestPrice

session = Session()

try:
    games = session.query(Game).all()

    inserted = 0
    updated = 0

    for game in games:
        latest = (
            session.query(GamePrice)
            .filter(GamePrice.game_id == game.id)
            .order_by(GamePrice.timestamp.desc())
            .first()
        )

        if not latest:
            continue

        existing = (
            session.query(GameLatestPrice)
            .filter(GameLatestPrice.game_id == game.id)
            .first()
        )

        if existing:
            existing.game_name = latest.game_name
            existing.price = latest.price
            existing.original_price = latest.original_price
            existing.discount_percent = latest.discount_percent
            existing.current_players = latest.current_players
            existing.store_url = latest.store_url
            existing.timestamp = latest.timestamp
            updated += 1
        else:
            row = GameLatestPrice(
                game_id=game.id,
                game_name=latest.game_name,
                price=latest.price,
                original_price=latest.original_price,
                discount_percent=latest.discount_percent,
                current_players=latest.current_players,
                store_url=latest.store_url,
                timestamp=latest.timestamp,
            )
            session.add(row)
            inserted += 1

    session.commit()
    print(f"Inserted {inserted} latest rows")
    print(f"Updated {updated} latest rows")

finally:
    session.close()
