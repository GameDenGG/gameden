from scraper.steam_scraper import get_game_price_data

url = "https://store.steampowered.com/app/1245620/ELDEN_RING/"
result = get_game_price_data(url)
print(result)