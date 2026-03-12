# GameDen.gg Domain Go-Live Checklist

This checklist is for launching production on:

`https://gameden.gg`

## Required Environment Values

- `DISPLAY_SITE_NAME=GameDen.gg`
- `SITE_URL=https://gameden.gg`
- `CANONICAL_HOST_REDIRECT=true` (recommended for production)
- `CANONICAL_REDIRECT_HOSTS=www.gameden.gg`
- `CORS_ALLOW_ALL_ORIGINS=false`
- `CORS_ALLOW_ORIGINS` includes:
  - `https://gameden.gg`
  - `https://www.gameden.gg`
  - local development origins (`localhost` / `127.0.0.1`) as needed

## Domain + Metadata Verification

- `GET /robots.txt` returns `Sitemap: https://gameden.gg/sitemap.xml`
- `GET /sitemap.xml` contains `https://gameden.gg/...` URLs
- `GET /site.webmanifest` returns `name=GameDen.gg` and app metadata
- Frontend pages include canonical, Open Graph, and Twitter metadata for `gameden.gg`
- Frontend runtime metadata script is loaded from `/site-config.js` and `/web/site-branding.js`

## Canonical Host Behavior

- `www.gameden.gg` should issue `308` redirect to apex `gameden.gg` when `CANONICAL_HOST_REDIRECT=true`
- Apex (`gameden.gg`) remains the canonical production host

## Regression Safety

- Snapshot/cache-backed API usage remains unchanged
- No raw-history request-time analytics introduced
- Local development still works with default localhost origins in CORS allowlist
