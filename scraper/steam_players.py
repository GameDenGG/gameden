import re
import os
import time
from typing import Optional

import requests

from config import STEAM_USER_AGENT
from logger_config import setup_logger

logger = setup_logger("steam_players")

HEADERS = {
    "User-Agent": STEAM_USER_AGENT
}

APP_ID_PATTERN = re.compile(r"/app/(\d+)(?:/|$)", re.IGNORECASE)
REQUEST_TIMEOUT_SECONDS = 15
REQUEST_RETRIES = max(0, int(os.getenv("STEAM_REQUEST_RETRIES", "2")))


def extract_app_id(url: str) -> Optional[str]:
    if not url:
        return None

    match = APP_ID_PATTERN.search(url)
    if match:
        return match.group(1)

    return None


def get_current_players(url: str) -> Optional[int]:
    app_id = extract_app_id(url)
    if not app_id:
        logger.warning("Could not extract app id from url: %s", url)
        return None

    endpoint = "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"
    params = {"appid": app_id}

    data = None
    for attempt in range(1, REQUEST_RETRIES + 2):
        try:
            response = requests.get(
                endpoint,
                headers=HEADERS,
                params=params,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            data = response.json()
            break
        except requests.RequestException:
            if attempt > REQUEST_RETRIES:
                logger.exception("Steam current players request failed for app %s", app_id)
                return None
            delay = 0.35 * attempt
            logger.warning("Steam players retry app_id=%s attempt=%s delay=%.2fs", app_id, attempt, delay)
            time.sleep(delay)
        except ValueError:
            logger.exception("Steam current players returned invalid JSON for app %s", app_id)
            return None

    if data is None:
        return None

    response_data = data.get("response", {})
    if not isinstance(response_data, dict):
        logger.warning("Unexpected current players response shape for app %s", app_id)
        return None

    player_count = response_data.get("player_count")

    if player_count is None:
        logger.info("No current player count available for app %s", app_id)
        return None

    try:
        player_count = int(player_count)
    except (TypeError, ValueError):
        logger.warning("Invalid current player count for app %s: %s", app_id, player_count)
        return None

    logger.info("Current players for app %s: %s", app_id, player_count)
    return player_count
