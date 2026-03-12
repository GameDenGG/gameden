import requests
from bs4 import BeautifulSoup
import time

url = "https://store.steampowered.com/app/570/Dota_2/"

headers = {
    "User-Agent": "Mozilla/5.0"
}

def get_price():
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, "lxml")

    # Steam price container (may change for different games)
    price = soup.find("div", class_="game_purchase_price")

    if price:
        print("Price:", price.text.strip())
    else:
        print("Price not found")

# Track price every 60 seconds (testing version only)
while True:
    get_price()
    time.sleep(60)

