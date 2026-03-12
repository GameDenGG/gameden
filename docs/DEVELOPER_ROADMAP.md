# NEWWORLD Steam Deals Platform
## Developer Roadmap

---

# Goal

Turn the current backend-complete deal platform into a polished, production-ready product with strong UX, SEO, and growth loops.

---

# Current State

Completed backend systems:
- price ingestion scheduler
- snapshot refresh worker
- homepage cache
- search
- released games pagination
- price history
- player history
- deal detection
- leaderboards
- watchlists + price targets
- alerts + notifications
- metrics + health

That means the highest-value remaining work is mostly frontend and deployment.

---

# Phase 1 — Frontend Core Experience

## 1. Homepage polish
Build or finish sections for:
- Trending Deals
- Biggest Price Drops
- New Historical Lows
- Recommended Deals
- Most Played Deals
- Upcoming Releases

## 2. Deal discovery page
Implement frontend for:
- `/deals/search`
- interactive filters
- sort controls
- pagination
- saved filter presets

## 3. Game details page
Build:
- price history chart
- player activity chart
- deal event timeline
- wishlist / price target actions

## 4. Quick View modal
Show:
- price chart
- player chart
- tags / genres
- historical low status
- recommendation badges

---

# Phase 2 — User Retention

## 5. Wishlist UX
Allow:
- add/remove wishlist
- price target controls
- alert inbox
- unread alert count

## 6. Push notifications UX
Implement:
- enable notifications prompt
- subscription save flow
- notification settings

## 7. “Deals for You” personalization
Expose backend recommendation score in UI:
- recommended row
- “because you wishlisted X”
- “target hit” banners

---

# Phase 3 — SEO / Growth

## 8. Global leaderboard pages
Create public pages:
- Top Deals Today
- New Historical Lows
- Biggest Price Drops
- Most Played Deals
- Trending Deals

## 9. SEO category pages
Examples:
- Best Steam RPG Deals
- Best Co-op Deals
- Best Strategy Deals
- Best Deals Under $20

## 10. Social sharing
Add:
- Open Graph tags
- shareable deal cards
- leaderboard share pages

---

# Phase 4 — Production Hardening

## 11. Deployment
Deploy:
- API
- ingestion worker
- snapshot worker
- static/frontend

## 12. Background process supervision
Ensure workers auto-restart and log centrally.

## 13. Secrets / environment cleanup
Standardize:
- DATABASE_URL
- VAPID keys
- runtime config
- production / staging separation

## 14. Backup / migration process
Document:
- schema migrations
- Neon branching
- rollback strategy

---

# Phase 5 — Scale / Optimization

## 15. Search optimization
Tune `/search` and `/deals/search` once traffic grows.

## 16. API compression + ETags
Enable for heavy endpoints when needed.

## 17. Read replica support
Route heavy read endpoints to replica later.

## 18. Async ingestion fanout
Scale price ingestion when catalog expands toward 10k+ games.

---

# Recommended Build Order

1. Homepage UI
2. Deal discovery page
3. Game details page with charts
4. Wishlist + alerts UX
5. Leaderboard pages
6. Deployment
7. SEO pages
8. Scale optimizations

---

# Short-Term Next Steps

## Immediate
- keep ingestion worker running
- keep snapshot worker running
- verify cache freshness
- confirm deal events are appearing

## This week
- build deal explorer UI
- build price history chart component
- build player history chart component
- build alert center UI

## After that
- ship leaderboard pages
- connect push notifications end-to-end
- deploy production environment

---

# Success Checklist

A polished MVP is done when:
- homepage updates automatically
- users can discover and filter deals
- game pages show price + player charts
- users can wishlist games and set targets
- alerts are created and visible
- leaderboard pages are public
- workers run continuously in deployment

---

# Long-Term Vision

NEWWORLD becomes:
- a Steam deal tracker
- a discovery engine
- a price alert platform
- a trend analytics site
- an SEO-driven content surface

That combination is what makes platforms like SteamDB and GG.deals sticky.
