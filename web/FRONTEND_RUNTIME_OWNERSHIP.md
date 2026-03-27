# Frontend Runtime Ownership Map

This file exists to prevent edits from landing in similarly named but inactive/alternate implementations.

## Page Route -> Template Ownership

- `/` -> `web/index.html`
- `/all-results` and aliases (`/best-deals`, `/historical-lows`, `/trending`, `/buy-now`, ...) -> `web/all-results.html`
- `/game` and `/game/{identifier}` -> `web/game.html` (canonical game detail page)
- `/history` -> `web/history.html`
- `/watchlist` -> `web/watchlist.html`
- `/game-detail` -> `web/game-detail.html` (alternate/legacy detail page, not canonical `/game`)

Source of truth: route handlers in `api/server.py`.

## Chart Runtime Ownership

### Canonical game detail charts (`/game`, `/game/{identifier}`)
- Active file: `web/game.html`
- Active chart functions:
  - `renderPriceChart(historyPayload)`
  - `renderPlayerChart(historyPayload, options = {})`
  - `getAuthoritativePlayerDisplaySeries(...)`
  - `computePlayerTrendInsights(...)`
  - `playerSignalPlugin` (inside `renderPlayerChart`)

### History page charts (`/history`)
- Active file: `web/history.html`
- Active chart function:
  - `buildCharts(rows)`
- Active chart instances:
  - `priceChartInstance`
  - `playersChartInstance`

### Alternate legacy game detail chart path (`/game-detail`)
- Active file: `web/game-detail.js` (loaded by `web/game-detail.html`)
- Active chart function:
  - `renderPriceChart(data)`
- This path does not own the canonical `/game` player/price chart behavior.

## Overlap Notes

- `renderPriceChart(...)` exists in both `web/game.html` and `web/game-detail.js`.
  - For `/game` bugs, edit `web/game.html`.
  - For `/game-detail` bugs, edit `web/game-detail.js`.
- `/history` chart code is separate from `/game` chart code and should be treated independently.
