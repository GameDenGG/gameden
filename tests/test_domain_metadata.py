from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WEB_DIR = ROOT / "web"


class DomainMetadataContractTests(unittest.TestCase):
    def test_frontend_pages_use_gameden_canonical_metadata(self) -> None:
        page_expectations = {
            "index.html": "https://gameden.gg/",
            "all-results.html": "https://gameden.gg/web/all-results.html",
            "game-detail.html": "https://gameden.gg/web/game-detail.html",
            "game.html": "https://gameden.gg/web/game.html",
            "history.html": "https://gameden.gg/web/history.html",
            "watchlist.html": "https://gameden.gg/watchlist",
        }

        for file_name, canonical_url in page_expectations.items():
            with self.subTest(file_name=file_name):
                page_text = (WEB_DIR / file_name).read_text(encoding="utf-8")
                self.assertIn(f'<link rel="canonical" href="{canonical_url}">', page_text)
                self.assertIn(f'<meta property="og:url" content="{canonical_url}">', page_text)
                self.assertIn('<meta property="og:site_name" content="GameDen.gg">', page_text)
                self.assertIn('<link rel="icon" type="image/x-icon" href="/web/favicon.ico">', page_text)
                self.assertIn('<link rel="manifest" href="/site.webmanifest">', page_text)
                self.assertIn('<script src="/site-config.js"></script>', page_text)
                self.assertIn('<script src="/web/site-branding.js"></script>', page_text)

    def test_backend_has_canonical_domain_routes_and_config_usage(self) -> None:
        server_text = (ROOT / "api" / "server.py").read_text(encoding="utf-8")
        self.assertIn('app = FastAPI(title=f"{SITE_NAME} API", description=SITE_DESCRIPTION)', server_text)
        self.assertIn('@app.get("/robots.txt", include_in_schema=False)', server_text)
        self.assertIn('@app.get("/sitemap.xml", include_in_schema=False)', server_text)
        self.assertIn('@app.get("/site-config.js", include_in_schema=False)', server_text)
        self.assertIn('@app.get("/site.webmanifest", include_in_schema=False)', server_text)
        self.assertIn("@app.middleware(\"http\")", server_text)
        self.assertIn("CANONICAL_HOST_REDIRECT", server_text)
        self.assertIn("CANONICAL_REDIRECT_HOSTS", server_text)
        self.assertIn("CORS_ALLOW_ORIGINS", server_text)

    def test_env_example_includes_site_domain_configuration(self) -> None:
        env_example = (ROOT / ".env.example").read_text(encoding="utf-8")
        self.assertIn("DISPLAY_SITE_NAME=GameDen.gg", env_example)
        self.assertIn("SITE_URL=https://gameden.gg", env_example)
        self.assertIn("CANONICAL_HOST_REDIRECT=false", env_example)
        self.assertIn("CANONICAL_REDIRECT_HOSTS=www.gameden.gg", env_example)
        self.assertIn("CORS_ALLOW_ORIGINS=https://gameden.gg", env_example)
        self.assertIn("CORS_ALLOW_ALL_ORIGINS=false", env_example)


if __name__ == "__main__":
    unittest.main()
