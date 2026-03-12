from database.models import Session, Game

games = [
    {
        "appid": "570",
        "name": "Dota 2",
        "store_url": "https://store.steampowered.com/app/570/Dota_2/"
    },
    {
        "appid": "730",
        "name": "Counter-Strike 2",
        "store_url": "https://store.steampowered.com/app/730/CounterStrike_2/"
    },
    {
        "appid": "1245620",
        "name": "Elden Ring",
        "store_url": "https://store.steampowered.com/app/1245620/ELDEN_RING/"
    }
]

session = Session()

for g in games:
    existing = session.query(Game).filter_by(appid=g["appid"]).first()

    if not existing:
        game = Game(
            appid=g["appid"],
            name=g["name"],
            store_url=g["store_url"]
        )
        session.add(game)
        print("Added:", g["name"])
    else:
        print("Already exists:", g["name"])

session.commit()
session.close()